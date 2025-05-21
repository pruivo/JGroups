package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.NullAddress;
import org.jgroups.View;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.stack.MessageProcessingPolicy;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;
import static org.jgroups.conf.AttributeType.SCALAR;
import static org.jgroups.protocols.TP.MSG_OVERHEAD;
import static org.jgroups.util.MessageBatch.Mode.OOB;
import static org.jgroups.util.MessageBatch.Mode.REG;

/**
 * Queues messages per destination ('null' is a special destination), sending when the last sender thread to the same
 * destination returns or max_size has been reached. This uses 1 thread per destination, so it won't scale to many
 * cluster members (unless virtual threads are used).
 * <br/>
 * See https://issues.redhat.com/browse/JGRP-2639 for details.
 * @author Bela Ban
 * @since  5.2.7
 */
@Experimental
public class PerDestinationBundler implements Bundler {

    private static final int MAX_QUEUE_SIZE = 8192;
    private static final int MAX_DRAIN_QUEUE_SIZE = 1024;

    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    @Property(name="max_size", type= AttributeType.BYTES,
      description="Maximum number of bytes for messages to be queued (per destination) until they are sent")
    protected int                           max_size=64000;

    @Property(description="When the queue is full, senders will drop a message rather than wait until space " +
      "is available (https://issues.redhat.com/browse/JGRP-2765)")
    protected boolean                       drop_when_full=true;

    @ManagedAttribute(description="Total number of messages sent (single and batches)",type=AttributeType.SCALAR)
    protected final LongAdder               total_msgs_sent=new LongAdder();

    @ManagedAttribute(description="Number of single messages sent",type=AttributeType.SCALAR)
    protected final LongAdder               num_single_msgs_sent=new LongAdder();

    @ManagedAttribute(description="Number of batches sent",type=AttributeType.SCALAR)
    protected final LongAdder               num_batches_sent=new LongAdder();

    @ManagedAttribute(description="Number of batches sent because no more messages were available",type=AttributeType.SCALAR)
    protected final LongAdder               num_send_due_to_no_msgs=new LongAdder();

    @ManagedAttribute(description="Number of batches sent because the queue was full",type=AttributeType.SCALAR)
    protected final LongAdder               num_sends_due_to_max_size=new LongAdder();

    @ManagedAttribute(description="Number of dropped messages (when drop_when_full is true)",type=SCALAR)
    protected final LongAdder               num_drops_on_full_queue=new LongAdder();

    @ManagedAttribute(description="Times to send messages")
    protected final AverageMinMax           send_times=new AverageMinMax().unit(TimeUnit.NANOSECONDS);

    protected TP                            transport;
    protected MsgStats                      msg_stats;
    protected MessageProcessingPolicy       msg_processing_policy;
    protected Log                           log;
    protected Address                       local_addr;
    protected final Map<Address, BaseQueue> dests = Util.createConcurrentMap();
    protected static final Address          NULL=new NullAddress();
    protected static final String           THREAD_NAME="pd-bundler";

    public int     size() {
        return dests.values().stream().map(BaseQueue::size).reduce(0, Integer::sum);
    }
    public int     getQueueSize()         {return -1;}
    public int     getMaxSize()           {return max_size;}
    public Bundler setMaxSize(int s)      {this.max_size=s; return this;}

    @ManagedAttribute(description="Average number of messages in an BatchMessage")
    public double avgBatchSize() {
        long num_batches=num_batches_sent.sum(), total_msgs=total_msgs_sent.sum(), single_msgs=num_single_msgs_sent.sum();
        if(num_batches == 0 || total_msgs == 0) return 0.0;
        long batched_msgs=total_msgs - single_msgs;
        return batched_msgs / (double)num_batches;
    }

    @Override public void resetStats() {
        Stream.of(total_msgs_sent, num_batches_sent, num_single_msgs_sent, num_sends_due_to_max_size, num_drops_on_full_queue)
          .forEach(LongAdder::reset);
        send_times.clear();
    }

    public void init(TP transport) {
        this.transport=Objects.requireNonNull(transport);
        msg_processing_policy=transport.msgProcessingPolicy();
        msg_stats=transport.getMessageStats();
        this.log=transport.getLog();
    }

    public void start() {
        local_addr=Objects.requireNonNull(transport.getAddress());
        dests.values().forEach(BaseQueue::start);
    }

    public void stop() {
        dests.values().forEach(BaseQueue::stop);
    }

    public void send(Message msg) throws Exception {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
        Address dest=msg.dest() == null ? NULL : msg.dest();
        var buf = dests.get(dest);
        if(buf == null)
            buf = storeQueueIfAbsent(dest);
        buf.send(msg);
    }

    public void viewChange(View view) {
        List<Address> mbrs=view.getMembers();
        if(mbrs == null) return;

        mbrs.stream()
                .filter(this::isMemberMissing)
                .forEach(this::storeQueueIfAbsent);

        // remove left members
        dests.keySet().stream()
                .filter(Predicate.not(Predicate.isEqual(NULL)))
                .filter(Predicate.not(mbrs::contains))
                .map(dests::remove)
                .filter(Objects::nonNull)
                .forEach(BaseQueue::stop);
    }

    private boolean isMemberMissing(Address address) {
        if (address == null || address == NULL) {
            // multicast is never absent
            return false;
        }
        return !dests.containsKey(address);
    }

    protected BaseQueue storeQueueIfAbsent(Address destination) {
        assert destination != null;
        return dests.computeIfAbsent(destination, this::createAndStartQueue);
    }

    protected BaseQueue createAndStartQueue(Address destination) {
        assert destination != null;
        return Objects.equals(local_addr, destination) || Objects.equals(destination, transport.getPhysicalAddress()) ?
                new LoopbackQueue().start() :
                new RemoteQueue(destination == NULL ? null : destination).start();
    }

    protected void loopback(Address dest, Collection<Message> list) {
        MessageBatch reg = null, oob = null;
        for (Message msg : list) {
            if (msg.isFlagSet(DONT_LOOPBACK))
                continue;
            if (msg.isFlagSet(Message.Flag.OOB)) {
                // we cannot reuse message batches (like in ReliableMulticast.removeAndDeliver()), because batches are
                // submitted to a thread pool and new calls of this method might change them while they're being passed up
                if (oob == null)
                    oob = new MessageBatch(dest, local_addr, transport.getClusterNameAscii(), dest == null, OOB, list.size());
                oob.add(msg);
            } else {
                if (reg == null)
                    reg = new MessageBatch(dest, local_addr, transport.getClusterNameAscii(), dest == null, REG, list.size());
                reg.add(msg);
            }
        }
        if (reg != null) {
            msg_stats.received(reg);
            msg_processing_policy.loopback(reg, false);
        }
        if (oob != null) {
            msg_stats.received(oob);
            msg_processing_policy.loopback(oob, true);
        }
    }

    protected void sendBundledMessages(Address dst, ByteArrayDataOutputStream output, int msgCount, int resetPosition) {
        long start = transport.statsEnabled() ? System.nanoTime() : 0;
        try {
            transport.doSend(output.buffer(), 0, output.position(), dst);
            transport.getMessageStats().incrNumBatchesSent();
            num_batches_sent.increment();
        } catch (Throwable e) {
            log.error("%s: failed sending message to %s: %s", local_addr, dst, e);
        } finally {
            if (start > 0) {
                send_times.add(System.nanoTime() - start);
            }
            total_msgs_sent.add(msgCount);
            output.position(resetPosition);
        }
    }

    protected abstract class BaseQueue implements Runnable {
        protected final Address dest;
        protected final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
        protected final List<Message> drain_queue = new ArrayList<>(MAX_DRAIN_QUEUE_SIZE);
        private volatile Thread bundler_thread;
        protected volatile boolean running;

        protected BaseQueue(Address dest) {
            this.dest = dest;
        }

        protected void send(Message msg) throws InterruptedException {
            if (!running) {
                return;
            }
            if (drop_when_full || msg.isFlagSet(Message.TransientFlag.DONT_BLOCK)) {
                if (!queue.offer(msg)) {
                    num_drops_on_full_queue.increment();
                }
                return;
            }
            queue.put(msg);
        }

        int size() {
            return queue.size();
        }

        protected BaseQueue start() {
            if (running)
                stop();
            bundler_thread = transport.getThreadFactory().newThread(this, THREAD_NAME);
            running = true;
            bundler_thread.start();
            return this;
        }

        protected void stop() {
            Thread tmp = bundler_thread;
            running = false;
            if (tmp != null)
                tmp.interrupt();
        }
    }

    private static void prepareOutput(int msgCount, ByteArrayDataOutputStream output, int resetPosition) {
        // move the position back to write the message counter.
        var pos = output.position();
        assert pos > resetPosition;
        output.position(resetPosition - Global.INT_SIZE);
        output.writeInt(msgCount);
        output.position(pos);
    }

    protected class RemoteQueue extends BaseQueue implements Consumer<Message> {

        protected final ByteArrayDataOutputStream output;
        protected final int resetIndex;
        protected final boolean multicast;
        private int count = 0;

        protected RemoteQueue(Address dest) {
            super(dest);
            output = new ByteArrayDataOutputStream(max_size + MSG_OVERHEAD);
            multicast = dest == null;
            // The header never changes as the source and destination are always the same.
            // We can cache it and reset the ByteArrayDataOutputStream position to "resetIndex" on each iteration.
            try {
                Util.writeMessageListHeader(dest, local_addr, transport.cluster_name.chars(), 0, output, multicast);
            } catch (IOException e) {
                // should never happen!
                throw new IllegalStateException(e);
            }
            resetIndex = output.position();
        }

        @Override
        public void run() {
            while (running) {
                drain_queue.clear();
                try {
                    drain_queue.add(queue.take());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                queue.drainTo(drain_queue);
                drain_queue.forEach(this);

                // flush what is left
                if (count > 0) {
                    flush();
                }

                if (multicast) {
                    // Loopback the messages before we clear the queue.
                    // We don't care about max_size, send everything up!
                    loopback(dest, drain_queue);
                }
            }
        }

        @Override
        public void accept(Message msg) {
            var pos = output.position();
            if (pos + msg.size() > max_size) {
                // max size reached
                flush();
                pos = output.position();
            }

            try {
                output.writeShort(msg.getType());
                msg.writeToNoAddrs(local_addr, output);
                ++count;
            } catch (IOException e) {
                // remove this message from the buffer
                output.position(pos);
            }
        }

        private void flush() {
            prepareOutput(count, output, resetIndex);
            sendBundledMessages(dest, output, count, resetIndex);
            count = 0;
            num_sends_due_to_max_size.increment();
        }
    }

    protected class LoopbackQueue extends BaseQueue {

        protected LoopbackQueue() {
            super(local_addr);
        }

        @Override
        public void run() {
            while (running) {
                drain_queue.clear();
                try {
                    drain_queue.add(queue.take());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                queue.drainTo(drain_queue);
                // We don't care about max_size, send everything up!
                loopback(local_addr, drain_queue);
            }
        }
    }
}

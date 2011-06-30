package org.jgroups.protocols.pmcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.groups.failure.FinalMessagesManager;
import org.jgroups.groups.GroupAddress;
import org.jgroups.groups.GroupMulticastHeader;
import org.jgroups.groups.MessageID;
import org.jgroups.stack.Protocol;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Pedro
 */
public class GROUP_MULTICAST2 extends Protocol {

    //stats
    private long rcv_msg = 0, deliver_msgs = 0, proposes_msgs = 0;
    private long sent_group_msgs = 0, sent_uni_msgs = 0;

    //measure times
    private final AtomicLong multicastTime = new AtomicLong(0);
    private final AtomicLong unicastTime = new AtomicLong(0);
    private final AtomicLong newMsgTime = new AtomicLong(0);
    private final AtomicLong proposeMsgTime = new AtomicLong(0);
    private final AtomicLong finalMsgTime = new AtomicLong(0);
    private final AtomicLong deliverMsgTime = new AtomicLong(0);

    //self deliver stats
    private final AtomicLong self_deliver_time = new AtomicLong(0);
    private final AtomicLong number_of_messages = new AtomicLong(0);
    private final Map<Long,Long> self_del_times=new ConcurrentHashMap<Long,Long>();

    //id of messages and clock for timestamp the messages (and seqno)
    private long clock = 0;
    private final ReentrantLock clockLock = new ReentrantLock();
    private final AtomicLong message_id = new AtomicLong(0);
    private long seqno = 0;
    private final ReentrantLock seqnoLock = new ReentrantLock();

    //for view change
    private final AtomicReference<View> actualView = new AtomicReference<View>();
    private final FinalMessagesManager finalMsgMan = new FinalMessagesManager();
    //private final FailureManager failureManager = new FailureManager();

    //node info
    private Address local_addr = null;
    private final AtomicReference<Address> coordinator = new AtomicReference<Address>(null);

    //received and sended messages information
    private final Map<MessageID, ReceiverInfo> received = new HashMap<MessageID, ReceiverInfo>();
    private final ReentrantReadWriteLock receivedLock = new ReentrantReadWriteLock();
    private final ConcurrentMap<MessageID, SenderInfo> sended = new ConcurrentHashMap<MessageID, SenderInfo>();

    //threads to processing messages and outcoming messages
    private ExecutorService final_messages = Executors.newSingleThreadExecutor(new DefaultThreadFactory("Final-Messages"));
    private ExecutorService new_messages = Executors.newSingleThreadExecutor(new DefaultThreadFactory("New-Messages"));
    private ExecutorService propose = Executors.newSingleThreadExecutor(new DefaultThreadFactory("Propose-Timestamp"));
    private final Outcoming outcoming = new Outcoming();

    //log
    private boolean trace = false;
    private boolean error = false;
    private boolean debug = false;
    private boolean warn = false;

    @Override
    public void start() throws Exception {
        super.start();
        trace = log.isTraceEnabled();
        error = log.isErrorEnabled();
        debug = log.isDebugEnabled();
        warn = log.isWarnEnabled();
        finalMsgMan.start();
        outcoming.start();
        final_messages = Executors.newSingleThreadExecutor();
        propose = Executors.newSingleThreadExecutor();
    }

    @Override
    public void stop() {
        super.stop();
        received.clear();
        sended.clear();
        finalMsgMan.stop();
        outcoming.interrupt();
        shutdown();
        resetStats();
    }

    @ManagedOperation
    public void resetStats() {
        rcv_msg = deliver_msgs = proposes_msgs = sent_group_msgs = sent_uni_msgs = 0;
        multicastTime.set(0);
        unicastTime.set(0);
        newMsgTime.set(0);
        proposeMsgTime.set(0);
        finalMsgTime.set(0);
        deliverMsgTime.set(0);
        self_deliver_time.set(0);
        number_of_messages.set(0);
        self_del_times.clear();
    }

    @ManagedAttribute
    public long getReceivedMessages() {
        return rcv_msg;
    }

    @ManagedAttribute
    public long getDeliverMessages() {
        return deliver_msgs;
    }

    @ManagedAttribute
    public long getReceivedProposeMessage() {
        return proposes_msgs;
    }

    @ManagedAttribute
    public long getSentGroupMessages() {
        return sent_group_msgs;
    }

    @ManagedAttribute
    public long getSentUnicastMessages() {
        return sent_uni_msgs;
    }

    @ManagedAttribute
    public long getUnicastTime() {
        return unicastTime.get();
    }

    @ManagedAttribute
    public long getNewMsgTime() {
        return newMsgTime.get();
    }

    @ManagedAttribute
    public long getProposeMsgTime() {
        return proposeMsgTime.get();
    }

    @ManagedAttribute
    public long getFinalMsgTime() {
        return finalMsgTime.get();
    }

    @ManagedAttribute
    public long getDeliverMsgTime() {
        return deliverMsgTime.get();
    }

    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> m=super.dumpStats();
        m.put("messages_received", rcv_msg);
        m.put("messages_deliver", deliver_msgs);
        m.put("proposes_received", proposes_msgs);
        m.put("sent_group_messages", sent_group_msgs);
        m.put("sent_unicast_messages", sent_uni_msgs);
        m.put("multicast_time", multicastTime.get());
        m.put("unicast_time", unicastTime.get());
        m.put("new_msg_time", newMsgTime.get());
        m.put("propose_msg_time", proposeMsgTime.get());
        m.put("final_msg_time", finalMsgTime.get());
        m.put("deliver_msg_time", deliverMsgTime.get());
        return m;
    }

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }

    //--- stop messages --- //
    void shutdown() {
        final_messages.shutdown();
        propose.shutdown();
        new_messages.shutdown();
        final_messages.shutdownNow();
        propose.shutdownNow();
        new_messages.shutdownNow();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address addr = msg.getDest();
                if(addr != null && addr.isGroupAddress()) {
                    Set<Address> destination = ((GroupAddress)addr).getAddresses();
                    View view = actualView.get();
                    destination.retainAll(view.getMembers());

                    if(!msg.isFlagSet(Message.NO_TOTAL_ORDER)) {
                        boolean oneDest = destination.size() == 1;

                        GroupMulticastHeader hdr = new GroupMulticastHeader(local_addr, message_id.incrementAndGet());
                        hdr.setType(GroupMulticastHeader.MESSAGE);
                        MessageID msgID = hdr.getID();

                        if(oneDest) {
                            hdr.setOneDestinationFlag();
                        }

                        msg.putHeader(this.id, hdr);

                        if(trace) {
                            log.trace("Multicast message with total order passes down. message_id=" + msgID +
                                    ",view_id=" + view.getViewId());
                        }

                        if(!oneDest) {
                            sended.put(msgID, new SenderInfo(destination));
                        }

                        if(statsEnabled()) {
                            self_del_times.put(msgID.getId(), System.nanoTime());
                        }
                    } else if(trace) {
                        log.trace("Multicast message without total order passes down");
                    }
                    multicast(msg, destination, true);
                    return null;
                }

                if(trace) {
                    log.trace("Broadcast or unicast message passes down");
                }

                break;

            case Event.VIEW_CHANGE:
                View oldView;
                View newView = (View)evt.getArg();
                do{
                    oldView = actualView.get();
                } while(!actualView.compareAndSet(oldView, newView));
                handleViewChange(oldView, newView);
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        Message msg;
        GroupMulticastHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                hdr=(GroupMulticastHeader)msg.getHeader(this.id);

                if(hdr == null || msg.isFlagSet(Message.NO_TOTAL_ORDER)) {
                    if(debug) {
                        log.debug("Header is null or flag NO_TOTAL_ORDER is enabled");
                    }
                    break; // pass up
                }

                switch(hdr.getType()) {
                    case GroupMulticastHeader.COORDINATOR_REQUEST:
                        if(trace) {
                            log.trace("Received a coordinator request message");
                        }
                        if(amICoordinator()) {
                            processMessagesToOrder(msg, hdr);
                            return null;
                        }
                        if(warn) {
                            log.warn("Coordinator request message received, but I'm not the coordinator");
                        }
                        return null;
                    case GroupMulticastHeader.COORDINATOR_RESPONSE:
                        if(trace) {
                            log.trace("Received a coordinator response message");
                        }
                        processMessages(msg, hdr);
                        return null;
                    case GroupMulticastHeader.MESSAGE:
                        //deliver!
                        if(hdr.isOneDestinationFlagSet()) {
                            deliver(msg);
                            return null;
                        }

                        processNewMsg(msg, hdr);
                        return null;
                    case GroupMulticastHeader.BUNDLE_MESSAGE:
                        processBundleMsg(msg, hdr);
                        return null;
                    default:
                        if(error) {
                            log.error("Unkown header type received=" + hdr.getType());
                        }
                        break;
                }
                return null;

            case Event.VIEW_CHANGE:
                View oldView;
                View newView = (View)evt.getArg();
                do{
                    oldView = actualView.get();
                } while(!actualView.compareAndSet(oldView, newView));
                Object retval = up_prot.up(evt);
                handleViewChange(oldView, newView);
                return retval;
        }

        return up_prot.up(evt);
    }

    /*
    -------------------------private methods-----------------------------------------
     */

    private boolean amICoordinator() {
        return local_addr.equals(getCoordinator());
    }

    private Address getCoordinator() {
        return coordinator.get();
    }

    /* -------------------- METHODS TO HANDLE MESSAGES AND VIEWS -------------------- */

    //messages from the other nodes
    private void processMessagesToOrder(Message msg, GroupMulticastHeader hdr) {
        //no-op
    }

    //message from coordinator with the new order
    private void processMessages(Message msg, GroupMulticastHeader hdr) {
        //no-op
    }

    private void handleViewChange(View oldView, View newView) {
        //no-op
    }

    private void processNewMsg(Message msg, GroupMulticastHeader hdr) {
        //log.warn("new");
        new_messages.execute(new NewMessage(msg, hdr));
    }

    private void processBundleMsg(Message msg, GroupMulticastHeader hdr) {
        SendInformation si = (SendInformation) msg.getObject();
        Address from = msg.getSrc();
        propose.execute(new ProposeTSProcess(from, si.getProposeTs()));
        final_messages.execute(new FinalTSProcess(from, si.getFinalTs()));
    }

    //version 3 -- optimization (one thread is touching in array)
    private void deliverAllPossibleMessages() {
        //log.warn("deliver all possible messages");
        long start = System.nanoTime();
        try {
            if(trace) {
                log.trace("try deliver all possible messages");
            }

            do {
                long minimumTs = Long.MAX_VALUE;
                MessageID minimumMsgID = null;
                ReceiverInfo minimumRI = null;

                try {
                    receivedLock.writeLock().lock();
                    for(Map.Entry<MessageID, ReceiverInfo> entry : received.entrySet()) {
                        ReceiverInfo ri = entry.getValue();
                        MessageID msgID = entry.getKey();
                        long ts = ri.getTimestamp();

                        if(ts < minimumTs || (ts == minimumTs && msgID.compareTo(minimumMsgID) < 0)) {
                            minimumTs = ts;
                            minimumMsgID = msgID;
                            minimumRI = ri;
                        }
                    }
                } finally {
                    receivedLock.writeLock().unlock();
                }

                if(minimumRI != null && minimumRI.isFinal()) {
                    received.remove(minimumMsgID);

                    Message toDeliver = minimumRI.getMessage();
                    toDeliver.setSrc(minimumRI.getOrigin());

                    if(statsEnabled() && minimumMsgID.getAddress().equals(local_addr)) {
                        Long start2 = self_del_times.get(minimumMsgID.getId());
                        if(start2 != null) {
                            long time = start2 - System.nanoTime();
                            self_deliver_time.addAndGet(time);
                            number_of_messages.incrementAndGet();
                        }
                    }

                    deliver(toDeliver);
                } else {
                    break;
                }
            } while(true) ;
        } finally {
            deliverMsgTime.addAndGet(System.nanoTime() - start);
        }
    }

    /* ------------------- METHODS FOR SEND AND DELIVER MESSAGES ------------------- */

    private void multicast(Message msg, Collection<Address> destination, boolean fifo) {
        long start = System.nanoTime();
        try {
            if(msg.getSrc() == null) {
                msg.setSrc(local_addr);
            }

            //avoid FIFO deliver! only reliable
            if(!fifo) {
                msg.setFlag(Message.OOB);
            }

            GroupMulticastHeader hdr = (GroupMulticastHeader) msg.getHeader(this.id);
            if(trace) {
                log.trace("send message in multicast. msg=" + msg + ",destination=" + destination + (hdr != null ? ",msg_id=" + hdr.getID() : ""));
            }

            boolean needAdquireLock = hdr != null;

            try {
                if(needAdquireLock) {
                    seqnoLock.lock();
                    seqno++;
                    hdr.setTimestamp(seqno);
                }

                for(Address addr : destination) {
                    Message copy = msg.copy();
                    copy.setDest(addr);
                    unicast(copy, fifo);
                }
            } finally {
                if(needAdquireLock) {
                    seqnoLock.unlock();
                }
            }
        } finally {
            multicastTime.addAndGet(System.nanoTime() - start);
        }
        sent_group_msgs++;
    }

    private void unicast(Message msg, boolean fifo) {
        long start = System.nanoTime();
        try {
            if(msg.getSrc() == null) {
                msg.setSrc(local_addr);
            }

            if(!fifo) {
                msg.setFlag(Message.OOB);
            }

            if(trace) {
                log.trace("send message in unicast to " + msg.getDest());
            }
            down_prot.down(new Event(Event.MSG, msg));
        } finally {
            unicastTime.addAndGet(System.nanoTime() - start);
        }
        sent_uni_msgs++;
    }

    private void deliver(Message msg) {
        Address sender=msg.getSrc();
        GroupMulticastHeader hdr = (GroupMulticastHeader) msg.getHeader(this.id);
        if(sender == null) {
            if(error) {
                log.error("sender is null,cannot deliver msg " + msg);
            }
            return;
        }

        if(trace) {
            log.trace("deliver message " + msg  + (hdr != null ? ",msg_id=" + hdr.getID() : ""));
        }

        up_prot.up(new Event(Event.MSG, msg));
    }

    //------------------------ Runnable for the two types of messages --------------------------- //
    private class NewMessage implements Runnable {
        private Message message;
        private GroupMulticastHeader hdr;

        public NewMessage(Message message, GroupMulticastHeader hdr) {
            this.message = message;
            this.hdr = hdr;
        }

        @Override
        public void run() {
            long start = System.nanoTime();
            try {
                MessageID msgID = hdr.getID();
                Address from = message.getSrc();
                Address origin = hdr.getOrigin();

                if(trace) {
                    log.trace("incoming new message. msg_id=" + msgID + ";from=" + from + ";origin=" + origin);
                }

                if(!from.equals(origin)) {
                    if(warn) {
                        log.warn("received new message but it wasn't send by the original sender. discart it. msg_id=" + msgID);
                    }
                    return;
                }

                if(received.containsKey(msgID)) {
                    if(warn) {
                        log.warn("received new message but the message already exists. discart it. msg_id=" + msgID);
                    }
                } else {
                    long ts;
                    try {
                        clockLock.lock();
                        clock = clock + 1;
                        ts = clock;
                    } finally {
                        clockLock.unlock();
                    }

                    ReceiverInfo msgInfo = new ReceiverInfo(message);
                    msgInfo.setTemporaryTimestamp(ts);

                    try {
                        receivedLock.readLock().lock();
                        received.put(msgID, msgInfo);
                    } finally {
                        receivedLock.readLock().unlock();
                    }

                    outcoming.addInformation(origin, true, msgID.getId(), ts);
                    if(trace) {
                        log.trace("send timestamp propose msg_id=" + msgID + ",to=" + origin + ",ts=" + ts);
                    }
                }
            } finally {
                newMsgTime.addAndGet(System.nanoTime() - start);
                rcv_msg++;
            }
        }
    }

    private class FinalTSProcess implements Runnable {
        private Map<Long, Long> finalTs;
        private Address src;

        public FinalTSProcess(Address from, Map<Long, Long> finalTs) {
            this.src = from;
            this.finalTs = finalTs;
        }

        @Override
        public void run() {
            MessageID otherMsgID = new MessageID(src);
            boolean needTryToDeliver = false;

            for(Map.Entry<Long, Long> entry : finalTs.entrySet()) {
                otherMsgID.setID(entry.getKey());
                long ts = entry.getValue();

                ReceiverInfo ri = received.get(otherMsgID);
                if(ri != null) {
                    needTryToDeliver = true;
                    ri.setFinalTimestamp(ts);

                    try {
                        clockLock.lock();
                        if(ts > clock) {
                            clock = ts;
                        }
                    } finally {
                        clockLock.unlock();
                    }


                } else {
                    log.warn("received a final ts to a message that i dont received: " + otherMsgID);
                }
            }
            if(needTryToDeliver) {
                deliverAllPossibleMessages();
            }
        }
    }

    private class ProposeTSProcess implements Runnable {

        private Map<Long, Long> proposeTs;
        private Address src;

        public ProposeTSProcess(Address from, Map<Long, Long> proposeTs) {
            this.src = from;
            this.proposeTs = proposeTs;
        }

        @Override
        public void run() {
            MessageID myMsgID = new MessageID(local_addr);

            List<Address> dests = new ArrayList<Address>();
            List<Long> ids = new ArrayList<Long>();
            List<Long> tss = new ArrayList<Long>();

            for(Map.Entry<Long, Long> entry : proposeTs.entrySet()) {
                myMsgID.setID(entry.getKey());
                long ts = entry.getValue();

                SenderInfo sent = sended.get(myMsgID);
                if(sent != null) {
                    sent.addTimestamp(ts, src);
                    if(sent.isFinal()) {
                        long id = entry.getKey();
                        long finalTs = sent.getGreatestTimestampReceived();
                        for(Address addr : sent.getDestination()) {
                            dests.add(addr);
                            ids.add(id);
                            tss.add(finalTs);
                        }
                        sended.remove(myMsgID);
                    }
                }
            }
            outcoming.addInformation(dests, ids, tss);
        }
    }

    /* ----------------------------------- RECEIVER INFO ---------------------------------------------*/
    //information about received messages
    public class ReceiverInfo implements Comparable<ReceiverInfo>{
        private Message message;
        GroupMulticastHeader header;
        private volatile boolean temporary;
        private long timestamp;

        public ReceiverInfo(Message msg) {
            this.message = msg;
            this.header = (GroupMulticastHeader) msg.getHeader(id);
            temporary = true;
            timestamp = -1L;
        }

        public boolean isFinal() {
            return !temporary;
        }

        public Message getMessage() {
            return message;
        }

        public void setFinalTimestamp(long ts) {
            if(temporary) {
                if(ts < timestamp && warn) {
                    log.warn("set final timestamp to a small number for " + getMessageID());
                }
                timestamp = ts;
                temporary = false;
            }
        }

        public void setTemporaryTimestamp(long ts) {
            if(temporary && ts > timestamp) {
                timestamp = ts;
            }
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Address getOrigin() {
            return header != null ? header.getOrigin() : null;
        }

        public MessageID getMessageID() {
            return header != null ? header.getID() : null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ReceiverInfo that = (ReceiverInfo) o;

            MessageID thisID = this.getMessageID();
            MessageID otherID = that.getMessageID();

            return (thisID != null ? thisID.equals(otherID) : otherID == null || otherID.equals(thisID));
        }

        @Override
        public int hashCode() {
            MessageID thisID = this.getMessageID();

            return thisID != null ? thisID.hashCode() : 0;
        }

        @Override
        public int compareTo(ReceiverInfo other) {
            if(other == null) {
                return -1;
            } else if(this == other) {
                return 0;
            } else if(other.getTimestamp() == this.getTimestamp()) {
                return this.getMessageID().compareTo(other.getMessageID());
            }

            return new Long(this.getTimestamp()).compareTo(other.getTimestamp());
        }

        @Override
        public String toString() {
            return "[ReceiverInfo:msg_id=" + getMessageID() + ",ts=" + timestamp + ",isFinal?" + !temporary + "]";
        }
    }

    /* ----------------------------------- SENDER INFO ---------------------------------------------*/
    //information about sended messages
    public class SenderInfo {
        private long greatestTimestampReceived = -1L;
        private Set<Address> destsLeft;
        private Set<Address> destination;

        public SenderInfo(Collection<Address> destination) {
            this.destsLeft = new HashSet<Address>(destination);
            this.destination = new HashSet<Address>(destination);
        }

        public long getGreatestTimestampReceived() {
            return greatestTimestampReceived;
        }

        public void addTimestamp(long ts, Address from) {
            if(ts < 0) {
                return;
            }

            if(destsLeft.remove(from)) {
                if(ts > greatestTimestampReceived) {
                    greatestTimestampReceived = ts;
                }
            }
        }

        public boolean isFinal() {
            return destsLeft.isEmpty();
        }

        public Set<Address> getDestination() {
            return destination;
        }

        public synchronized void handleFailure(Collection<Address> failedNodes) {
            destsLeft.removeAll(failedNodes);
            destination.removeAll(failedNodes);
        }
    }

    //----------------------- sends messges to network (make some bundling) ------------------ //
    private class Outcoming extends Thread {
        private final List<SendInformation> infoToSend = new ArrayList<SendInformation>();
        private final ReentrantLock lock = new ReentrantLock();
        private Condition empty;
        private final AtomicBoolean acceptNewMessages = new AtomicBoolean(false);

        public Outcoming() {
            super("GMcast-Outcoming");
            empty = lock.newCondition();
        }

        public void addInformation(Address destination, boolean propose, long id, long ts) {
            if(!acceptNewMessages.get()) {
                return;
            }
            try {
                lock.lock();
                SendInformation si = new SendInformation(destination);
                int idx = infoToSend.indexOf(si);
                if(idx >= 0) {
                    si = infoToSend.get(idx);
                } else {
                    infoToSend.add(si); //added to the end of list
                }

                if(propose) {
                    si.addTemporaryTs(id, ts);
                } else {
                    si.addFinalTs(id, ts);
                }

                empty.signal();
            } finally {
                lock.unlock();
            }
        }

        //all have the same size!!
        public void addInformation(List<Address> destinations, List<Long> ids, List<Long> tss) {
            if(!acceptNewMessages.get() || destinations.isEmpty()) {
                return;
            }
            try {
                lock.lock();
                int size = destinations.size();
                while(size-- > 0) {
                    addInformation(destinations.remove(0), false, ids.remove(0), tss.remove(0));
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void run() {
            while(!isInterrupted() && isAlive()) {
                SendInformation si = null;
                try {
                    lock.lock();
                    while(infoToSend.size() == 0) {
                        empty.await();
                    }
                    si = infoToSend.remove(0); //removes the first element!
                } catch (InterruptedException e) {
                    acceptNewMessages.set(false);
                    clean();
                    continue;
                } finally {
                    lock.unlock();
                }

                try {
                    seqnoLock.lock();
                    seqno++;

                    Message msg = new Message(si.getDestination(), local_addr, si);
                    GroupMulticastHeader hdr = new GroupMulticastHeader();
                    hdr.setType(GroupMulticastHeader.BUNDLE_MESSAGE);
                    hdr.setTimestamp(seqno);
                    msg.putHeader(id, hdr);

                    unicast(msg, false);
                } catch(Exception e) {
                    //be quiet....
                    log.warn("shhhh " + e);
                } finally {
                    seqnoLock.unlock();
                }
            }
        }

        @Override
        public void start() {
            acceptNewMessages.set(true);
            super.start();
        }

        @Override
        public void interrupt() {
            acceptNewMessages.set(false);
            super.interrupt();
            clean();
        }

        private void clean() {
            infoToSend.clear();
        }
    }

    public static class SendInformation implements Serializable {
        private transient Address destination; //we don't want to send duplicated information =)
        private Map<Long, Long> proposeTs;
        private Map<Long, Long> finalTs;

        public SendInformation(Address destination) {
            this.destination = destination;
            proposeTs = new HashMap<Long, Long>();
            finalTs = new HashMap<Long, Long>();
        }

        public void addTemporaryTs(long id, long ts) {
            proposeTs.put(id, ts);
        }

        public void addFinalTs(long id, long ts) {
            finalTs.put(id, ts);
        }

        public Address getDestination() {
            return destination;
        }

        public Map<Long, Long> getProposeTs() {
            return proposeTs;
        }

        public Map<Long, Long> getFinalTs() {
            return finalTs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SendInformation that = (SendInformation) o;
            return (destination != null ? destination.equals(that.destination) : that.destination == null);
        }

        @Override
        public int hashCode() {
            return destination != null ? destination.hashCode() : 0;
        }
    }

    private static class DefaultThreadFactory implements ThreadFactory {
        String prefix;

        public DefaultThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, prefix);
        }
    }
}
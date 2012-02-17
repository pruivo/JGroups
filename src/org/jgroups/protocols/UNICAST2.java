package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.*;
import org.jgroups.util.AgeOutCache;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Reliable unicast layer. Implemented with negative acks. Every sender keeps its messages in an AckSenderWindow. A
 * receiver stores incoming messages in a NakReceiverWindow, and asks the sender for retransmission if a gap is
 * detected. Every now and then (stable_interval), a timer task sends a STABLE message to all senders, including the
 * highest received and delivered seqnos. A sender purges messages lower than highest delivered and asks the STABLE
 * sender for messages it might have missed (smaller than highest received). A STABLE message can also be sent when
 * a receiver has received more than max_bytes from a given sender.<p/>
 * The advantage of this protocol over {@link org.jgroups.protocols.UNICAST} is that it doesn't send acks for every
 * message. Instead, it sends 'acks' after receiving max_bytes and/ or periodically (stable_interval).
 * @author Bela Ban
 */
@MBean(description="Reliable unicast layer")
public class UNICAST2 extends Protocol implements Retransmitter.RetransmitCommand, AgeOutCache.Handler<Address> {
    public static final long DEFAULT_FIRST_SEQNO=Global.DEFAULT_FIRST_UNICAST_SEQNO;


    /* ------------------------------------------ Properties  ------------------------------------------ */
    
    private int[] timeout= { 400, 800, 1600, 3200 }; // for NakSenderWindow: max time to wait for missing acks

    /**
     * The first value (in milliseconds) to use in the exponential backoff
     * retransmission mechanism. Only enabled if the value is > 0
     */
    @Property(description="The first value (in milliseconds) to use in the exponential backoff. Enabled if greater than 0")
    private int exponential_backoff=300;


    @Property(description="Max number of messages to be removed from a NakReceiverWindow. This property might " +
            "get removed anytime, so don't use it !")
    private int max_msg_batch_size=50000;

    @Property(description="Max number of bytes before a stability message is sent to the sender")
    protected long max_bytes=10000000;

    @Property(description="Max number of milliseconds before a stability message is sent to the sender(s)")
    protected long stable_interval=60000L;

    @Property(description="Max number of STABLE messages sent for the same highest_received seqno. A value < 1 is invalid")
    protected int max_stable_msgs=5;

    @Property(description="Number of rows of the matrix in the retransmission table (only for experts)",writable=false)
    int xmit_table_num_rows=5;

    @Property(description="Number of elements of a row of the matrix in the retransmission table (only for experts). " +
      "The capacity of the matrix is xmit_table_num_rows * xmit_table_msgs_per_row",writable=false)
    int xmit_table_msgs_per_row=10000;

    @Property(description="Resize factor of the matrix in the retransmission table (only for experts)",writable=false)
    double xmit_table_resize_factor=1.2;

    @Property(description="Number of milliseconds after which the matrix in the retransmission table " +
      "is compacted (only for experts)",writable=false)
    long xmit_table_max_compaction_time=10 * 60 * 1000;

    @Property(description="If enabled, the removal of a message from the retransmission table causes an " +
      "automatic purge (only for experts)",writable=false)
    boolean xmit_table_automatic_purging=true;

    @Property(description="Whether to use the old retransmitter which retransmits individual messages or the new one " +
      "which uses ranges of retransmitted messages. Default is true. Note that this property will be removed in 3.0; " +
      "it is only used to switch back to the old (and proven) retransmitter mechanism if issues occur")
    private boolean use_range_based_retransmitter=true;

    @Property(description="Time (in milliseconds) after which an idle incoming or outgoing connection is closed. The " +
      "connection will get re-established when used again. 0 disables connection reaping")
    protected long conn_expiry_timeout=60000;
    /* --------------------------------------------- JMX  ---------------------------------------------- */


    private long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0, num_xmits=0;


    /* --------------------------------------------- Fields ------------------------------------------------ */

    private final ConcurrentMap<Address,SenderEntry>   send_table=Util.createConcurrentMap();
    private final ConcurrentMap<Address,ReceiverEntry> recv_table=Util.createConcurrentMap();

    protected final ReentrantLock recv_table_lock=new ReentrantLock();

    private final List<Address> members=new ArrayList<Address>(11);

    private Address local_addr=null;

    private TimeScheduler timer=null; // used for retransmissions (passed to AckSenderWindow)

    private boolean started=false;

    private short last_conn_id=0;

    protected long max_retransmit_time=60 * 1000L;

    private AgeOutCache<Address> cache=null;

    private Future<?> stable_task_future=null; // bcasts periodic STABLE message (added to timer below)

    protected Future<?> connection_reaper; // closes idle connections


    public int[] getTimeout() {return timeout;}

    @Property(name="timeout",converter=PropertyConverters.IntegerArray.class,description="list of timeouts")
    public void setTimeout(int[] val) {
        if(val != null)
            timeout=val;
    }

    public void setMaxMessageBatchSize(int size) {
        if(size >= 1)
            max_msg_batch_size=size;
    }

    @ManagedAttribute
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}

    @ManagedAttribute
    public String getMembers() {return members != null? members.toString() : "[]";}


    @ManagedAttribute(description="Returns the number of outgoing (send) connections")
    public int getNumSendConnections() {
        return send_table.size();
    }

    @ManagedAttribute(description="Returns the number of incoming (receive) connections")
    public int getNumReceiveConnections() {
        return recv_table.size();
    }

    @ManagedAttribute(description="Returns the total number of outgoing (send) and incoming (receive) connections")
    public int getNumConnections() {
        return getNumReceiveConnections() + getNumSendConnections();
    }

    @ManagedOperation
    public String printConnections() {
        StringBuilder sb=new StringBuilder();
        if(!send_table.isEmpty()) {
            sb.append("send connections:\n");
            for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }

        if(!recv_table.isEmpty()) {
            sb.append("\nreceive connections:\n");
            for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }
        return sb.toString();
    }

    @ManagedAttribute(description="Whether the ConnectionReaper task is running")
    public boolean isConnectionReaperRunning() {return connection_reaper != null && !connection_reaper.isDone();}


    @ManagedAttribute
    public long getNumMessagesSent() {
        return num_msgs_sent;
    }

    @ManagedAttribute
    public long getNumMessagesReceived() {
        return num_msgs_received;
    }

    @ManagedAttribute
    public long getNumBytesSent() {
        return num_bytes_sent;
    }

    @ManagedAttribute
    public long getNumBytesReceived() {
        return num_bytes_received;
    }

    @ManagedAttribute
    public long getNumberOfRetransmissions() {
        return num_xmits;
    }

    @ManagedAttribute
    public long getPendingXmitRequests() {
        long retval=0;
        Collection<ReceiverEntry> values=recv_table.values();
        for(ReceiverEntry entry: values)
        if(entry.received_msgs != null)
            retval+=entry.received_msgs.getPendingXmits();
        return retval;
    }

    public long getMaxRetransmitTime() {
        return max_retransmit_time;
    }

    @Property(description="Max number of milliseconds we try to retransmit a message to any given member. After that, " +
            "the connection is removed. Any new connection to that member will start with seqno #1 again. 0 disables this")
    public void setMaxRetransmitTime(long max_retransmit_time) {
        this.max_retransmit_time=max_retransmit_time;
        if(cache != null && max_retransmit_time > 0)
            cache.setTimeout(max_retransmit_time);
    }

    @ManagedAttribute
    public int getAgeOutCacheSize() {
        return cache != null? cache.size() : 0;
    }

    @ManagedOperation
    public String printAgeOutCache() {
        return cache != null? cache.toString() : "n/a";
    }

    public AgeOutCache<Address> getAgeOutCache() {
        return cache;
    }

    @ManagedAttribute
    public int getNumberOfMessagesInReceiveWindows() {
        int num=0;
        for(ReceiverEntry entry: recv_table.values()) {
            if(entry.received_msgs != null)
                num+=entry.received_msgs.size();
        }
        return num;
    }


    @ManagedOperation(description="Returns the sizes of all NakReceiverWindow.RetransmitTables")
    public String printRetransmitTableSizes() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            NakReceiverWindow win=entry.getValue().received_msgs;
            sb.append(entry.getKey() + ": ").append(win.getRetransmitTableSize())
              .append(" (capacity=" + win.getRetransmitTableCapacity())
              .append(", fill factor=" + win.getRetransmitTableFillFactor() + "%)\n");
        }
        return sb.toString();
    }


    @ManagedOperation(description="Compacts the retransmission tables")
    public void compact() {
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            NakReceiverWindow win=entry.getValue().received_msgs;
            win.compact();
        }
    }

    @ManagedOperation(description="Purges highes delivered messages and compacts the retransmission tables")
    public void purgeAndCompact() {
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            NakReceiverWindow win=entry.getValue().received_msgs;
            win.stable(win.getHighestDelivered());
            win.compact();
        }
    }


    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=num_xmits=0;
    }


    public Map<String, Object> dumpStats() {
        Map<String, Object> m=super.dumpStats();
        m.put("num_msgs_sent", num_msgs_sent);
        m.put("num_msgs_received", num_msgs_received);
        m.put("num_bytes_sent", num_bytes_sent);
        m.put("num_bytes_received", num_bytes_received);
        m.put("num_xmits", num_xmits);
        m.put("num_msgs_in_recv_windows", getNumberOfMessagesInReceiveWindows());
        return m;
    }


    public TimeScheduler getTimer() {
        return timer;
    }

    /**
     * Only used for unit tests, don't use !
     * @param timer
     */
    public void setTimer(TimeScheduler timer) {
        this.timer=timer;
    }

    public void init() throws Exception {
        super.init();
        if(max_stable_msgs < 1)
            throw new IllegalArgumentException("max_stable_msgs ( " + max_stable_msgs + ") must be > 0");
        if(max_bytes <= 0)
            throw new IllegalArgumentException("max_bytes has to be > 0");
    }

    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        if(max_retransmit_time > 0)
            cache=new AgeOutCache<Address>(timer, max_retransmit_time, this);
        started=true;
        if(stable_interval > 0)
            startStableTask();
        if(conn_expiry_timeout > 0)
            startConnectionReaper();
    }

    public void stop() {
        started=false;
        stopStableTask();
        stopConnectionReaper();
        removeAllConnections();
    }


    public Object up(Event evt) {
        Message        msg;
        Address        dst, src;
        Unicast2Header hdr;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                dst=msg.getDest();

                if(dst == null || msg.isFlagSet(Message.NO_RELIABILITY))  // only handle unicast messages
                    break;  // pass up

                // changed from removeHeader(): we cannot remove the header because if we do loopback=true at the
                // transport level, we will not have the header on retransmit ! (bela Aug 22 2006)
                hdr=(Unicast2Header)msg.getHeader(this.id);
                if(hdr == null)
                    break;
                src=msg.getSrc();
                switch(hdr.type) {
                    case Unicast2Header.DATA:      // received regular message
                        handleDataReceived(src, hdr.seqno, hdr.conn_id, hdr.first, msg, evt);
                        return null; // we pass the deliverable message up in handleDataReceived()
                    case Unicast2Header.XMIT_REQ:  // received ACK for previously sent message
                        handleXmitRequest(src, hdr.seqno, hdr.high_seqno);
                        break;
                    case Unicast2Header.SEND_FIRST_SEQNO:
                        handleResendingOfFirstMessage(src, hdr.seqno);
                        break;
                    case Unicast2Header.STABLE:
                        stable(msg.getSrc(), hdr.conn_id, hdr.seqno, hdr.high_seqno);
                        break;
                    default:
                        log.error("UnicastHeader type " + hdr.type + " not known !");
                        break;
                }
                return null;
        }

        return up_prot.up(evt);   // Pass up to the layer above us
    }



    public Object down(Event evt) {
        switch (evt.getType()) {

            case Event.MSG: // Add UnicastHeader, add to AckSenderWindow and pass down
                Message msg=(Message)evt.getArg();
                Address dst=msg.getDest();

                /* only handle unicast messages */
                if (dst == null || msg.isFlagSet(Message.NO_RELIABILITY))
                    break;

                if(!started) {
                    if(log.isTraceEnabled())
                        log.trace("discarded message as start() has not yet been called, message: " + msg);
                    return null;
                }

                SenderEntry entry=send_table.get(dst);
                if(entry == null) {
                    entry=new SenderEntry(getNewConnectionId());
                    SenderEntry existing=send_table.putIfAbsent(dst, entry);
                    if(existing != null)
                        entry=existing;
                    else {
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": created connection to " + dst + " (conn_id=" + entry.send_conn_id + ")");
                        if(cache != null && !members.contains(dst))
                            cache.add(dst);
                    }
                }

                long seqno=-2;
                short send_conn_id=-1;
                Unicast2Header hdr;

                entry.lock(); // threads will only sync if they access the same entry
                try {
                    seqno=entry.sent_msgs_seqno;
                    send_conn_id=entry.send_conn_id;
                    hdr=Unicast2Header.createDataHeader(seqno, send_conn_id, seqno == DEFAULT_FIRST_SEQNO);
                    msg.putHeader(this.id, hdr);
                    entry.sent_msgs.addToMessages(seqno, msg);  // add *including* UnicastHeader, adds to retransmitter
                    entry.sent_msgs_seqno++;
                    entry.update();
                }
                finally {
                    entry.unlock();
                }

                if(log.isTraceEnabled()) {
                    StringBuilder sb=new StringBuilder();
                    sb.append(local_addr).append(" --> DATA(").append(dst).append(": #").append(seqno).
                            append(", conn_id=").append(send_conn_id);
                    if(hdr.first) sb.append(", first");
                    sb.append(')');
                    log.trace(sb);
                }

                try {
                    down_prot.down(evt);
                    num_msgs_sent++;
                    num_bytes_sent+=msg.getLength();
                }
                catch(Throwable t) {
                    log.warn("failed sending the message", t);
                }
                return null; // we already passed the msg down

            case Event.VIEW_CHANGE:  // remove connections to peers that are not members anymore !
                View view=(View)evt.getArg();
                List<Address> new_members=view.getMembers();
                Set<Address> non_members=new HashSet<Address>(send_table.keySet());
                non_members.addAll(recv_table.keySet());

                synchronized(members) {
                    members.clear();
                    if(new_members != null)
                        members.addAll(new_members);
                    non_members.removeAll(members);
                    if(cache != null) {
                        cache.removeAll(members);
                    }
                }

                if(!non_members.isEmpty()) {
                    if(log.isTraceEnabled())
                        log.trace("removing non members " + non_members);
                    for(Address non_mbr: non_members)
                        removeConnection(non_mbr);
                }
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        return down_prot.down(evt);          // Pass on to the layer below us
    }

    /**
     * Purge all messages in window for local_addr, which are <= low. Check if the window's highest received message is
     * > high: if true, retransmit all messages from high - win.high to sender
     * @param sender
     * @param highest_delivered
     * @param highest_seen
     */
    protected void stable(Address sender, short conn_id, long highest_delivered, long highest_seen) {
        SenderEntry entry=send_table.get(sender);
        AckSenderWindow win=entry != null? entry.sent_msgs : null;
        if(win == null)
            return;

        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" <-- STABLE(").append(sender)
                        .append(": ").append(highest_delivered).append("-")
                        .append(highest_seen).append(", conn_id=" + conn_id) +")");

        if(entry.send_conn_id != conn_id) {
            log.warn(local_addr + ": my conn_id (" + entry.send_conn_id +
                       ") != received conn_id (" + conn_id + "); discarding STABLE message !");
            return;
        }

        win.ack(highest_delivered);
        long win_high=win.getHighest();
        if(win_high > highest_seen) {
            for(long seqno=highest_seen; seqno <= win_high; seqno++) {
                Message msg=win.get(seqno); // destination is still the same (the member which sent the STABLE message)
                if(msg != null)
                    down_prot.down(new Event(Event.MSG, msg));
            }
        }
    }

    @ManagedOperation(description="Sends a STABLE message to all senders. This causes message purging and potential" +
      " retransmissions from senders")
    public void sendStableMessages() {
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            Address dest=entry.getKey();
            ReceiverEntry val=entry.getValue();
            NakReceiverWindow win=val != null? val.received_msgs : null;
            if(win != null) {
                long[] tmp=win.getDigest();
                long low=tmp[0], high=tmp[1];

                if(val.last_highest == high) {
                    if(val.num_stable_msgs >= val.max_stable_msgs) {
                        continue;
                    }
                    else
                        val.num_stable_msgs++;
                }
                else {
                    val.last_highest=high;
                    val.num_stable_msgs=1;
                }
                sendStableMessage(dest, val.recv_conn_id, low, high);
            }
        }
    }

    protected void sendStableMessage(Address dest, short conn_id, long low, long high) {
        Message stable_msg=new Message(dest, null, null);
        Unicast2Header hdr=Unicast2Header.createStableHeader(conn_id, low, high);
        stable_msg.putHeader(this.id, hdr);
        stable_msg.setFlag(Message.OOB);
        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" --> STABLE(").append(dest).append(": ")
              .append(low).append("-").append(high).append(", conn_id=").append(conn_id).append(")");
            log.trace(sb.toString());
        }
        down_prot.down(new Event(Event.MSG, stable_msg));

        ReceiverEntry entry=recv_table.get(dest);
        NakReceiverWindow win=entry != null? entry.received_msgs : null;
        if(win != null)
            win.stable(win.getHighestDelivered());
    }


    private void startStableTask() {
        if(stable_task_future == null || stable_task_future.isDone()) {
            final Runnable stable_task=new Runnable() {
                public void run() {
                    try {
                        sendStableMessages();
                    }
                    catch(Throwable t) {
                        log.error("sending of STABLE messages failed", t);
                    }
                }
            };
            stable_task_future=timer.scheduleWithFixedDelay(stable_task, stable_interval, stable_interval, TimeUnit.MILLISECONDS);
            if(log.isTraceEnabled())
                log.trace("stable task started");
        }
    }


    private void stopStableTask() {
        if(stable_task_future != null) {
            stable_task_future.cancel(false);
            stable_task_future=null;
        }
    }

    protected synchronized void startConnectionReaper() {
        if(connection_reaper == null || connection_reaper.isDone())
            connection_reaper=timer.scheduleWithFixedDelay(new ConnectionReaper(), conn_expiry_timeout, conn_expiry_timeout, TimeUnit.MILLISECONDS);
    }

    protected synchronized void stopConnectionReaper() {
        if(connection_reaper != null)
            connection_reaper.cancel(false);
    }

    /**
     * Removes and resets from connection table (which is already locked). Returns true if member was found, otherwise
     * false. This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    public void removeConnection(Address mbr) {
        removeSendConnection(mbr);
        removeReceiveConnection(mbr);
    }

    public void removeSendConnection(Address mbr) {
        SenderEntry entry=send_table.remove(mbr);
        if(entry != null)
            entry.reset();
    }

    public void removeReceiveConnection(Address mbr) {
        ReceiverEntry entry2=recv_table.remove(mbr);
        if(entry2 != null) {
            NakReceiverWindow win=entry2.received_msgs;
            if(win != null)
                sendStableMessage(mbr, entry2.recv_conn_id, win.getHighestDelivered(), win.getHighestReceived());
            entry2.reset();
        }
    }


    /**
     * This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    @ManagedOperation(description="Trashes all connections to other nodes. This is only used for testing")
    public void removeAllConnections() {
        for(SenderEntry entry: send_table.values())
            entry.reset();
        send_table.clear();

        sendStableMessages();
        for(ReceiverEntry entry2: recv_table.values())
            entry2.reset();
        recv_table.clear();
    }


    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        if(last_seqno < first_seqno)
            return;
        Unicast2Header hdr=Unicast2Header.createXmitReqHeader(first_seqno, last_seqno);
        Message xmit_req=new Message(sender, null, null);
        xmit_req.putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG,xmit_req));
    }

    
    /**
     * Called by AgeOutCache, to removed expired connections
     * @param key
     */
    public void expired(Address key) {
        if(key != null) {
            if(log.isDebugEnabled())
                log.debug("removing connection to " + key + " because it expired");
            removeConnection(key);
        }
    }



    /**
     * Check whether the hashtable contains an entry e for <code>sender</code> (create if not). If
     * e.received_msgs is null and <code>first</code> is true: create a new AckReceiverWindow(seqno) and
     * add message. Set e.received_msgs to the new window. Else just add the message.
     */
    protected void handleDataReceived(Address sender, long seqno, short conn_id, boolean first, Message msg, Event evt) {
        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" <-- DATA(").append(sender).append(": #").append(seqno);
            if(conn_id != 0) sb.append(", conn_id=").append(conn_id);
            if(first) sb.append(", first");
            sb.append(')');
            log.trace(sb);
        }

        ReceiverEntry entry;
        NakReceiverWindow win;

        recv_table_lock.lock();
        try {
            entry=recv_table.get(sender);
            win=entry != null? entry.received_msgs : null;
            if(first) {
                if(entry == null) {
                    entry=getOrCreateReceiverEntry(sender, seqno, conn_id);
                    win=entry.received_msgs;
                }
                else {  // entry != null && win != null
                    if(conn_id != entry.recv_conn_id) {
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": conn_id=" + conn_id + " != " + entry.recv_conn_id + "; resetting receiver window");

                        ReceiverEntry entry2=recv_table.remove(sender);
                        if(entry2 != null)
                            entry2.received_msgs.destroy();

                        entry=getOrCreateReceiverEntry(sender, seqno, conn_id);
                        win=entry.received_msgs;
                    }
                    else {
                        ;
                    }
                }
            }
            else { // entry == null && win == null OR entry != null && win == null OR entry != null && win != null
                if(win == null || entry.recv_conn_id != conn_id) {
                    recv_table_lock.unlock();
                    sendRequestForFirstSeqno(sender, seqno); // drops the message and returns (see below)
                    return;
                }
            }
        }
        finally {
            if(recv_table_lock.isHeldByCurrentThread())
                recv_table_lock.unlock();
        }

        entry.update();
        boolean added=win.add(seqno, msg); // win is guaranteed to be non-null if we get here
        num_msgs_received++;
        num_bytes_received+=msg.getLength();

        if(added) {
            int len=msg.getLength();
            if(len > 0) {
                boolean send_stable_msg=false;
                entry.lock();
                try {
                    entry.received_bytes+=len;
                    if(entry.received_bytes >= max_bytes) {
                        entry.received_bytes=0;
                        send_stable_msg=true;
                    }
                }
                finally {
                    entry.unlock();
                }

                if(send_stable_msg)
                    sendStableMessage(sender, entry.recv_conn_id, win.getHighestDelivered(), win.getHighestReceived());
            }
        }

        // An OOB message is passed up immediately. Later, when remove() is called, we discard it. This affects ordering !
        // http://jira.jboss.com/jira/browse/JGRP-377
        if(msg.isFlagSet(Message.OOB) && added) {
            try {
                up_prot.up(evt);
            }
            catch(Throwable t) {
                log.error("couldn't deliver OOB message " + msg, t);
            }
        }

        final AtomicBoolean processing=win.getProcessing();
        if(!processing.compareAndSet(false, true)) {
            return;
        }

        // try to remove (from the AckReceiverWindow) as many messages as possible and pass them up

        // Prevents concurrent passing up of messages by different threads (http://jira.jboss.com/jira/browse/JGRP-198);
        // this is all the more important once we have a concurrent stack (http://jira.jboss.com/jira/browse/JGRP-181),
        // where lots of threads can come up to this point concurrently, but only 1 is allowed to pass at a time
        // We *can* deliver messages from *different* senders concurrently, e.g. reception of P1, Q1, P2, Q2 can result in
        // delivery of P1, Q1, Q2, P2: FIFO (implemented by UNICAST) says messages need to be delivered only in the
        // order in which they were sent by their senders
        boolean released_processing=false;
        try {
            while(true) {
                List<Message> msgs=win.removeMany(processing, true, max_msg_batch_size); // remove my own messages
                if(msgs == null || msgs.isEmpty()) {
                    released_processing=true;
                    return;
                }

                for(Message m: msgs) {
                    // discard OOB msg: it has already been delivered (http://jira.jboss.com/jira/browse/JGRP-377)
                    if(m.isFlagSet(Message.OOB))
                        continue;
                    try {
                        up_prot.up(new Event(Event.MSG, m));
                    }
                    catch(Throwable t) {
                        log.error("couldn't deliver message " + m, t);
                    }
                }
            }
        }
        finally {
            // processing is always set in win.remove(processing) above and never here ! This code is just a
            // 2nd line of defense should there be an exception before win.remove(processing) sets processing
            if(!released_processing)
                processing.set(false);
        }
    }


    private ReceiverEntry getOrCreateReceiverEntry(Address sender, long seqno, short conn_id) {
        NakReceiverWindow win=new NakReceiverWindow(sender, this, seqno-1, timer, use_range_based_retransmitter,
                                                    xmit_table_num_rows, xmit_table_msgs_per_row,
                                                    xmit_table_resize_factor, xmit_table_max_compaction_time,
                                                    xmit_table_automatic_purging);

        if(exponential_backoff > 0)
            win.setRetransmitTimeouts(new ExponentialInterval(exponential_backoff));
        else
            win.setRetransmitTimeouts(new StaticInterval(timeout));

        ReceiverEntry entry=new ReceiverEntry(win, conn_id, max_stable_msgs);
        ReceiverEntry entry2=recv_table.putIfAbsent(sender, entry);
        if(entry2 != null)
            return entry2;
        if(log.isTraceEnabled())
            log.trace(local_addr + ": created receiver window for " + sender + " at seqno=#" + seqno + " for conn-id=" + conn_id);
        return entry;
    }


    private void handleXmitRequest(Address sender, long low, long high) {
        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" <-- XMIT(").append(sender).
                    append(": #").append(low).append( "-").append(high).append(')'));

        SenderEntry entry=send_table.get(sender);
        AckSenderWindow win=entry != null? entry.sent_msgs : null;
        if(win != null) {
            for(long i=low; i <= high; i++) {
                Message msg=win.get(i);
                if(msg == null) {
                    if(log.isWarnEnabled() && !local_addr.equals(sender)) {
                        StringBuilder sb=new StringBuilder();
                        sb.append("(requester=").append(sender).append(", local_addr=").append(this.local_addr);
                        sb.append(") message ").append(sender).append("::").append(i);
                        sb.append(" not found in retransmission table of ").append(sender).append(":\n").append(win);
                        log.warn(sb.toString());
                    }
                    continue;
                }
                
                down_prot.down(new Event(Event.MSG, msg));
                num_xmits++;
            }
        }
    }


    /**
     * We need to resend our first message with our conn_id
     * @param sender
     * @param seqno Resend messages in the range [lowest .. seqno]
     */
    private void handleResendingOfFirstMessage(Address sender, long seqno) {
        if(log.isTraceEnabled())
            log.trace(local_addr + " <-- SEND_FIRST_SEQNO(" + sender + "," + seqno + ")");
        SenderEntry entry=send_table.get(sender);
        AckSenderWindow win=entry != null? entry.sent_msgs : null;
        if(win == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender window for " + sender + " not found");
            return;
        }
        long lowest=win.getLowest();
        Message rsp=win.get(lowest);
        if(rsp == null)
            return;

        // We need to copy the UnicastHeader and put it back into the message because Message.copy() doesn't copy
        // the headers and therefore we'd modify the original message in the sender retransmission window
        // (https://jira.jboss.org/jira/browse/JGRP-965)
        Message copy=rsp.copy();
        Unicast2Header hdr=(Unicast2Header)copy.getHeader(this.id);
        Unicast2Header newhdr=hdr.copy();
        newhdr.first=true;
        copy.putHeader(this.id, newhdr);

        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" --> DATA(").append(copy.getDest()).append(": #").append(newhdr.seqno).
                    append(", conn_id=").append(newhdr.conn_id);
            if(newhdr.first) sb.append(", first");
            sb.append(')');
            log.trace(sb);
        }
        down_prot.down(new Event(Event.MSG, copy));

        if(++lowest > seqno)
            return;
        for(long i=lowest; i <= seqno; i++) {
            rsp=win.get(i);
            if(rsp != null)
                down_prot.down(new Event(Event.MSG, rsp));
        }
    }





    private short getNewConnectionId() {
        synchronized(this) {
            short retval=last_conn_id;
            if(last_conn_id >= Short.MAX_VALUE || last_conn_id < 0)
                last_conn_id=0;
            else
                last_conn_id++;
            return retval;
        }
    }

    private void sendRequestForFirstSeqno(Address dest, long seqno_received) {
        Message msg=new Message(dest);
        msg.setFlag(Message.OOB);
        Unicast2Header hdr=Unicast2Header.createSendFirstSeqnoHeader(seqno_received);
        msg.putHeader(this.id, hdr);
        if(log.isTraceEnabled())
            log.trace(local_addr + " --> SEND_FIRST_SEQNO(" + dest + "," + seqno_received + ")");
        down_prot.down(new Event(Event.MSG, msg));
    }

    @ManagedOperation(description="Closes connections that have been idle for more than conn_expiry_timeout ms")
    public void reapIdleConnections() {
        // remove expired connections from send_table
        for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
            SenderEntry val=entry.getValue();
            long age=val.age();
            if(age >= conn_expiry_timeout) {
                removeSendConnection(entry.getKey());
                if(log.isDebugEnabled())
                    log.debug(local_addr + ": removed expired connection for " + entry.getKey() +
                                " (" + age + " ms old) from send_table");
            }
        }

        // remove expired connections from recv_table
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            ReceiverEntry val=entry.getValue();
            long age=val.age();
            if(age >= conn_expiry_timeout) {
                removeReceiveConnection(entry.getKey());
                if(log.isDebugEnabled())
                    log.debug(local_addr + ": removed expired connection for " + entry.getKey() +
                                " (" + age + " ms old) from recv_table");
            }
        }
    }


    /**
     * The following types and fields are serialized:
     * <pre>
     * | DATA | seqno | conn_id | first |
     * | ACK  | seqno |
     * | SEND_FIRST_SEQNO | seqno |
     * </pre>
     */
    public static class Unicast2Header extends Header {
        public static final byte DATA             = 0;
        public static final byte XMIT_REQ         = 1;
        public static final byte SEND_FIRST_SEQNO = 2;
        public static final byte STABLE           = 3;

        byte    type;
        long    seqno;       // DATA, XMIT_REQ and STABLE
        long    high_seqno;  // XMIT_REQ and STABLE
        short   conn_id;     // DATA, STABLE
        boolean first;       // DATA


        public Unicast2Header() {} // used for externalization

        public static Unicast2Header createDataHeader(long seqno, short conn_id, boolean first) {
            return new Unicast2Header(DATA, seqno, 0L, conn_id, first);
        }

        public static Unicast2Header createXmitReqHeader(long low, long high) {
            if(low > high)
                throw new IllegalArgumentException("low (" + low + " needs to be <= high (" + high + ")");
            Unicast2Header retval=new Unicast2Header(XMIT_REQ, low);
            retval.high_seqno=high;
            return retval;
        }

        public static Unicast2Header createStableHeader(short conn_id, long low, long high) {
            if(low > high)
                throw new IllegalArgumentException("low (" + low + ") needs to be <= high (" + high + ")");
            Unicast2Header retval=new Unicast2Header(STABLE, low);
            retval.high_seqno=high;
            retval.conn_id=conn_id;
            return retval;
        }

        public static Unicast2Header createSendFirstSeqnoHeader(long seqno_received) {
            return new Unicast2Header(SEND_FIRST_SEQNO, seqno_received);
        }

        private Unicast2Header(byte type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }

        private Unicast2Header(byte type, long seqno, long high, short conn_id, boolean first) {
            this.type=type;
            this.seqno=seqno;
            this.high_seqno=high;
            this.conn_id=conn_id;
            this.first=first;
        }

        public byte getType() {
            return type;
        }

        public long getSeqno() {
            return seqno;
        }

        public long getHighSeqno() {
            return high_seqno;
        }

        public short getConnId() {
            return conn_id;
        }


        public boolean isFirst() {
            return first;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(type2Str(type)).append(", seqno=").append(seqno);
            if(conn_id != 0) sb.append(", conn_id=").append(conn_id);
            if(first) sb.append(", first");
            return sb.toString();
        }

        public static String type2Str(byte t) {
            switch(t) {
                case DATA:             return "DATA";
                case XMIT_REQ:         return "XMIT_REQ";
                case SEND_FIRST_SEQNO: return "SEND_FIRST_SEQNO";
                case STABLE:           return "STABLE";
                default:               return "<unknown>";
            }
        }

        public final int size() {
            int retval=Global.BYTE_SIZE; // type
            switch(type) {
                case DATA:
                    retval+=Util.size(seqno) // seqno
                      + Global.SHORT_SIZE    // conn_id
                      + Global.BYTE_SIZE;    // first
                    break;
                case XMIT_REQ:
                    retval+=Util.size(seqno, high_seqno);
                    break;
                case STABLE:
                    retval+=Util.size(seqno, high_seqno) + Global.SHORT_SIZE; // conn_id
                    break;
                case SEND_FIRST_SEQNO:
                    retval+=Util.size(seqno);
                    break;
            }
            return retval;
        }

        public Unicast2Header copy() {
            return new Unicast2Header(type, seqno, high_seqno, conn_id, first);
        }



        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            switch(type) {
                case DATA:
                    Util.writeLong(seqno, out);
                    out.writeShort(conn_id);
                    out.writeBoolean(first);
                    break;
                case XMIT_REQ:
                    Util.writeLongSequence(seqno, high_seqno, out);
                    break;
                case STABLE:
                    Util.writeLongSequence(seqno, high_seqno, out);
                    out.writeShort(conn_id);
                    break;
                case SEND_FIRST_SEQNO:
                    Util.writeLong(seqno, out);
                    break;
            }
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            switch(type) {
                case DATA:
                    seqno=Util.readLong(in);
                    conn_id=in.readShort();
                    first=in.readBoolean();
                    break;
                case XMIT_REQ:
                    long[] seqnos=Util.readLongSequence(in);
                    seqno=seqnos[0];
                    high_seqno=seqnos[1];
                    break;
                case STABLE:
                    seqnos=Util.readLongSequence(in);
                    seqno=seqnos[0];
                    high_seqno=seqnos[1];
                    conn_id=in.readShort();
                    break;
                case SEND_FIRST_SEQNO:
                    seqno=Util.readLong(in);
                    break;
            }
        }
    }



    private static final class SenderEntry {
        // stores (and retransmits) msgs sent by us to a certain peer
        final AckSenderWindow   sent_msgs;
        long                    sent_msgs_seqno=DEFAULT_FIRST_SEQNO;   // seqno for msgs sent by us
        final short             send_conn_id;
        private long            timestamp;
        final Lock              lock=new ReentrantLock();

        public SenderEntry(short send_conn_id) {
            this.send_conn_id=send_conn_id;
            sent_msgs=new AckSenderWindow();
            update();
        }

        void lock()   {lock.lock();}
        void unlock() {lock.unlock();}

        void reset() {
            if(sent_msgs != null)
                sent_msgs.reset();
            sent_msgs_seqno=DEFAULT_FIRST_SEQNO;
        }

        void update() {timestamp=System.currentTimeMillis();}
        long age() {return System.currentTimeMillis() - timestamp;}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(sent_msgs != null)
                sb.append(sent_msgs).append(", ");
            sb.append("send_conn_id=" + send_conn_id);
            sb.append(" (" + age() + " ms old)");
            return sb.toString();
        }
    }

    private static final class ReceiverEntry {
        private final NakReceiverWindow  received_msgs;  // stores all msgs rcvd by a certain peer in seqno-order
        private final short              recv_conn_id;
        private int                      received_bytes=0;
        private long                     timestamp;
        private final Lock               lock=new ReentrantLock();

        private long                     last_highest=-1;
        private int                      num_stable_msgs=0;
        public final int                 max_stable_msgs;



        public ReceiverEntry(NakReceiverWindow received_msgs, short recv_conn_id, final int max_stable_msgs) {
            this.received_msgs=received_msgs;
            this.recv_conn_id=recv_conn_id;
            this.max_stable_msgs=max_stable_msgs;
            update();
        }

        void lock()   {lock.lock();}
        void unlock() {lock.unlock();}

        void reset() {
            if(received_msgs != null)
                received_msgs.destroy();
            received_bytes=0;
            last_highest=-1;
            num_stable_msgs=0;
        }

        void update() {timestamp=System.currentTimeMillis();}
        long age() {return System.currentTimeMillis() - timestamp;}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(received_msgs != null)
                sb.append(received_msgs).append(", ");
            sb.append("recv_conn_id=" + recv_conn_id);
            sb.append(" (" + age() + " ms old)");
            return sb.toString();
        }
    }


    protected class ConnectionReaper implements Runnable {
        public void run() {
            reapIdleConnections();
        }
    }



}
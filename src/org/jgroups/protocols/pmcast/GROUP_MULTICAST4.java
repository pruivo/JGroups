package org.jgroups.protocols.pmcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.groups.DeliverManager;
import org.jgroups.groups.GroupAddress;
import org.jgroups.groups.GroupMulticastHeader;
import org.jgroups.groups.MessageID;
import org.jgroups.stack.Protocol;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Pedro
 */
@MBean(objectName="GroupMulticast", description="Provides atomic multicast properties")
public class GROUP_MULTICAST4 extends Protocol {

    //stats
    private final AtomicLong receivedMsgs = new AtomicLong(0);      //total number of received messages
    private final AtomicLong deliverMsgs = new AtomicLong(0);       //total number of delivered messages to application
    private final AtomicLong sentGroupMsgs = new AtomicLong(0);     //total number of ent group multicast
    private final AtomicLong sentUnicastMsgs = new AtomicLong(0);   //total number of sent unicast messages

    //measure times
    private final AtomicLong multicastTime = new AtomicLong(0);         //time "wasted" sending multicast messages
    private final AtomicLong processMessageTime = new AtomicLong(0);    //time "wasted" processing incoming messages
    private final AtomicLong deliverMsgTime = new AtomicLong(0);        //time "wasted" checking messages for deliver
    private final AtomicLong numTriesDeliver = new AtomicLong(0);       //number of times that tries deliver a message

    //self deliver stats
    private final AtomicLong selfDeliverTime = new AtomicLong(0);               //interval between application send() and deliver()
    private final AtomicLong selfDeliverWaitingQueueTime = new AtomicLong(0);   //time in queue before deliver, when the final order is known
    private final AtomicLong selfDeliverMsgs = new AtomicLong(0);       //number of self deliver messages
    private final AtomicLong appTimeProcessingMsg = new AtomicLong(0);  //time that the application takes to process a message
    //temporary maps (message_id => start timestamp) with the start timestamp for measure the times above
    private final Map<Long,Long> selfDelTimesPerMsg =new ConcurrentHashMap<Long,Long>();
    private final Map<Long,Long> selfDelWaitQueueTimesPerMsg =new ConcurrentHashMap<Long,Long>();

    //id of messages and clock for timestamp the messages
    private final ReentrantLock clockLock = new ReentrantLock();
    private final ReentrantLock seqNrLock = new ReentrantLock();
    private long clock = 0;
    private long seqNr = 0;
    private final AtomicLong message_id = new AtomicLong(0);
    private ExecutorService sender = Executors.newSingleThreadExecutor();

    //for view change
    private final AtomicReference<View> actualView = new AtomicReference<View>();

    //node info
    private Address local_addr = null;

    //received messages information. it contains the order, if it is final or not and the member that not sent the votes yet
    private final ConcurrentMap<MessageID, ReceivedMessageData> received = new ConcurrentHashMap<MessageID, ReceivedMessageData>();

    //to deliver messages: guarantees that only one thread deliver the message
    private final ReentrantLock deliverLock = new ReentrantLock();
    private final DeliverManager dm = new DeliverManager();
    private ExecutorService deliver = Executors.newSingleThreadExecutor();

    @Override
    public void start() throws Exception {
        super.start();
        resetStats();
        if(sender.isShutdown() || sender.isTerminated()) {
            sender = Executors.newSingleThreadExecutor();
        }
        if(deliver.isShutdown() || deliver.isTerminated()) {
            deliver = Executors.newSingleThreadExecutor();
        }
    }

    @Override
    public void stop() {
        super.stop();
        received.clear();
        dm.clear();
        sender.shutdown();
        sender.shutdownNow();
        deliver.shutdown();
        deliver.shutdownNow();
    }

    //------------------------------------- STATS AND JMX ---------------------------------------- //

    @ManagedOperation
    public void resetStats() {
        receivedMsgs.set(0);
        deliverMsgs.set(0);
        sentGroupMsgs.set(0);
        sentUnicastMsgs.set(0);
        multicastTime.set(0);
        processMessageTime.set(0);
        deliverMsgTime.set(0);
        selfDeliverTime.set(0);
        selfDeliverMsgs.set(0);
        numTriesDeliver.set(0);
        selfDelTimesPerMsg.clear();
        selfDelWaitQueueTimesPerMsg.clear();
        appTimeProcessingMsg.set(0);
    }

    @ManagedAttribute
    public long getReceivedMessages() {
        return receivedMsgs.get();
    }

    @ManagedAttribute
    public long getDeliverMessages() {
        return deliverMsgs.get();
    }

    @ManagedAttribute
    public long getSentGroupMessages() {
        return sentGroupMsgs.get();
    }

    @ManagedAttribute
    public long getProcessMessageTime() {
        return processMessageTime.get();
    }

    @ManagedAttribute
    public long getDeliverMsgTime() {
        return deliverMsgTime.get();
    }

    @ManagedAttribute
    public long getSelfDeliveredMessages() {
        return selfDeliverMsgs.get();
    }

    @ManagedAttribute
    public long getSelfDeliverTime() {
        return selfDeliverTime.get();
    }

    @ManagedAttribute
    public long getSelfDeliverWaitingTime() {
        return selfDeliverWaitingQueueTime.get();
    }

    @ManagedAttribute
    public int getQueueSize() {
        return received.size();
    }

    @ManagedAttribute
    public long getSentUnicastMessages() {
        return sentUnicastMsgs.get();
    }

    @ManagedAttribute
    public long getNumberTriesDeliver() {
        return numTriesDeliver.get();
    }

    @ManagedAttribute
    public long getAppTimeProcessing() {
        return appTimeProcessingMsg.get();
    }

    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> m=super.dumpStats();
        m.put("received_messages", receivedMsgs.get());
        m.put("deliver_messages", deliverMsgs.get());
        m.put("sent_group_messages", sentGroupMsgs.get());
        m.put("sent_unicast_messages", sentUnicastMsgs.get());
        m.put("multicast_time", multicastTime.get());
        m.put("process_message_time", processMessageTime.get());
        m.put("deliver_msg_time", deliverMsgTime.get());
        m.put("self_deliver_time", selfDeliverTime.get());
        m.put("self_deliver_waiting_time", selfDeliverWaitingQueueTime.get());
        m.put("self_delivered_messages", selfDeliverMsgs.get());
        m.put("number_tries_deliver",numTriesDeliver.get());
        m.put("app_time_processing", appTimeProcessingMsg.get());
        return m;
    }

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }

    //------------------------------------- END STATS AND JMX ---------------------------------------- //

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address addr = msg.getDest();

                if(addr != null && addr.isGroupAddress()) {
                    //it is a group multicast message
                    Set<Address> destination = ((GroupAddress)addr).getAddresses();
                    boolean loopback = true; //messages without total order *must* be send to myself
                    //messages only sends to myself can be delivered without pass by the protocol
                    boolean iAmOnlyMember = destination.size() == 1 && destination.contains(local_addr);

                    if(!msg.isFlagSet(Message.NO_TOTAL_ORDER) && !iAmOnlyMember) {
                        GroupMulticastHeader hdr = new GroupMulticastHeader(local_addr, message_id.incrementAndGet());
                        MessageID msgID = hdr.getID();

                        if(destination.contains(local_addr)) {
                            //we are in destination... so add my propose and put the information on 'received' set
                            ReceivedMessageData rmd = new ReceivedMessageData(destination, msg);
                            //this is the first message
                            received.put(msgID, rmd);
                            long ts;

                            try {
                                clockLock.lock();
                                ts = clock++;
                            } finally {
                                clockLock.unlock();
                            }

                            hdr.setTimestamp(ts);
                            hdr.setType(GroupMulticastHeader.MESSAGE_WITH_PROPOSE);
                            rmd.addPropose(local_addr,ts);

                            if(log.isTraceEnabled()) {
                                log.trace("multicast a message with myself in destination group. msg_id=" + msgID);
                            }
                            selfDelTimesPerMsg.put(msgID.getId(), System.nanoTime());
                            loopback = false;
                            dm.update(msgID, -1, ts);
                        } else {
                            hdr.setType(GroupMulticastHeader.MESSAGE);
                            if(log.isTraceEnabled()) {
                                log.trace("multicast a message without mysefl in destination group. msg_id=" + msgID);
                            }
                        }

                        hdr.addDestinations(destination);
                        msg.putHeader(this.id, hdr);
                    } else if(log.isTraceEnabled()) {
                        log.trace("multicast a message without total order");
                    }
                    multicast(msg, destination, loopback);
                    return null;
                }

                if(log.isTraceEnabled()) {
                    log.trace("broadcast or unicast message passes down");
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
                boolean trace = log.isTraceEnabled();

                if(hdr == null || msg.isFlagSet(Message.NO_TOTAL_ORDER)) {
                    if(trace) {
                        log.trace("header is null or flag NO_TOTAL_ORDER is enabled. ignore message and pass up");
                    }
                    break; // pass up
                }

                if(trace) {
                    log.trace("received a message of type: " + GroupMulticastHeader.type2String(hdr.getType()) +
                            ",msg_id=" + hdr.getID());
                }

                switch(hdr.getType()) {
                    case GroupMulticastHeader.MESSAGE:
                        processMessage(msg, hdr, false, true);
                        return null;
                    case GroupMulticastHeader.MESSAGE_WITH_PROPOSE:
                        processMessage(msg, hdr, true, true);
                        return null;
                    case GroupMulticastHeader.PROPOSE:
                        processMessage(msg, hdr, true, false);
                        return null;
                    default:
                        if(log.isWarnEnabled()) {
                            log.warn("unkown header type received for msg_id=" + hdr.getID());
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

    /* -------------------- METHODS TO HANDLE MESSAGES AND VIEWS -------------------- */

    private void handleViewChange(View oldView, View newView) {//TODO
    }

    private void processMessage(Message msg, GroupMulticastHeader hdr, boolean hasPropose, boolean hasMessage) {
        long start = System.nanoTime();
        boolean needToVote = false, needTryDeliver, trace = log.isTraceEnabled();
        MessageID msgID = hdr.getID();
        Address from = msg.getSrc();
        Address origin = hdr.getOrigin(); //the original sender

        long order = 0, oldOrder;
        try {
            if(log.isInfoEnabled()) {
                log.info("incoming message. msg_id=" + msgID + ";from=" + from + ";origin=" + origin + ";type=" +
                        GroupMulticastHeader.type2String(hdr.getType()));
            }

            ReceivedMessageData rmd = new ReceivedMessageData();
            ReceivedMessageData existing = received.putIfAbsent(msgID, rmd);
            if(existing != null) {
                //the message already exits in set... so we don't need to send the votes
                rmd = existing;
                if(trace) {
                    log.trace("message already received before. it's a vote. msg_id=" + msgID);
                }
            } else {
                needToVote = true; //first time
                if(trace) {
                    log.trace("first time received this message msg_id=" + msgID);
                }
            }

            //one thread for each entry...
            synchronized (rmd) {
                oldOrder = rmd.getOrder();
                rmd.putDestinationsIfAbsent(hdr.getDestinations());

                if(hasMessage) {
                    if(trace) {
                        log.trace("put message for msg_id=" + msgID);
                    }
                    rmd.putMessageIfAbsent(msg);
                }

                if(hasPropose) {
                    if(trace) {
                        log.trace("put propose for msg_id=" + msgID + ",propose=" + hdr.getTimestamp());
                    }
                    long ts = hdr.getTimestamp();
                    //the clock will be max(clock + 1, ts + 1)
                    try {
                        clockLock.lock();
                        if(ts >= clock) {
                            clock = ts + 1;
                        }
                    } finally {
                        clockLock.unlock();
                    }
                    rmd.addPropose(msg.getSrc(), hdr.getTimestamp());
                }

                if(needToVote) {
                    long ts = rmd.getOrder();

                    //the clock will be max(clock + 1, ts + 1)
                    try {
                        clockLock.lock();
                        long oldClock = clock;
                        if(ts == -1 || ts <= clock) {
                            ts = clock++;
                        } else {
                            clock = ts + 1;
                        }
                        if(log.isInfoEnabled()) {
                            log.info("send propose for " + msgID + ",my propose=" + ts +
                                    ",old clock=" + oldClock + ",new clock=" + clock);
                        }
                    } finally {
                        clockLock.unlock();
                    }

                    rmd.addPropose(local_addr, ts);
                }

                needTryDeliver = rmd.canDeliver(); //it's true when final order is known
                if(needTryDeliver && msgID.getAddress().equals(local_addr)) {
                    selfDelWaitQueueTimesPerMsg.put(msgID.getId(), System.nanoTime());
                }
                order = rmd.getOrder();

                if(oldOrder != order) {
                    if(needTryDeliver) {
                        dm.updateAndMarkCompleted(msgID, oldOrder, order);
                    } else {
                        dm.update(msgID, oldOrder, order);
                    }
                } else if(needTryDeliver) {
                    dm.markCompleted(msgID, oldOrder);
                }
                //log.warn("state: " + msgID + "=>" + rmd.state2String());
            }

        } finally {
            processMessageTime.addAndGet(System.nanoTime() - start);
            receivedMsgs.incrementAndGet();
        }

        if(needToVote) {
            GroupMulticastHeader hdr2 = hdr.copy();
            hdr2.setType(GroupMulticastHeader.PROPOSE);
            hdr2.setTimestamp(order);
            Message msg2 = new Message();
            msg2.putHeader(id, hdr2);
            multicast(msg2, hdr.getDestinations(), false);
        }

        if(needTryDeliver) { //only when the message becomes final, we try to deliver any messages
            deliverAllPossibleMessages_v3(order, msgID);
        }
    }

    //version 2
    //TODO need to be optimized
    /*private void deliverAllPossibleMessages(long order, MessageID messageID) {
        long start = System.nanoTime();
        boolean trace = log.isTraceEnabled();
        try {
            deliverLock.lock();

            if(trace) {
                log.trace("try deliver all possible messages");
            }

            do {
                int numberOfmessageWithSmallTs = 0;

                long minimumTs = Long.MAX_VALUE;
                MessageID minimumMsgID = null;
                for(Map.Entry<MessageID, ReceivedMessageData> entry : received.entrySet()) {
                    ReceivedMessageData ri = entry.getValue();
                    MessageID msgID = entry.getKey();
                    long ts = ri.getOrder();

                    if((ts != -1 && ts < minimumTs) || (ts == minimumTs && msgID.compareTo(minimumMsgID) < 0)) {
                        //this can be the next to deliver
                        minimumTs = ts;
                        minimumMsgID = msgID;
                    }

                    if(ts != -1 && ts < order) {
                        numberOfmessageWithSmallTs++;
                    }
                }

                if(log.isInfoEnabled()) {
                    log.info("+++ delivered message: " + messageID + ",order=" + order +
                            ",number of messages < order=" + numberOfmessageWithSmallTs +
                            ",size of queue=" + received.size() +
                            ",top message=" + minimumMsgID);
                }

                if(minimumMsgID == null) {
                    if(trace) {
                        log.trace("no messages found to deliver");
                    }
                    return;
                }

                ReceivedMessageData minimum = received.get(minimumMsgID);
                synchronized (minimum) {
                    if(log.isInfoEnabled()) {
                        log.info("State of the top message: " + minimum.state2String());
                    }
                    if(minimum.canDeliver() && minimumTs == minimum.getOrder()) {
                        received.remove(minimumMsgID);

                        Message toDeliver = minimum.getMessage();
                        toDeliver.setSrc(minimumMsgID.getAddress());

                        if(trace) {
                            log.trace("deliver message " + minimumMsgID + " with timstamp " + minimumTs);
                        }

                        //stats only!!
                        if(minimumMsgID.getAddress().equals(local_addr)) {
                            selfDeliverMsgs.incrementAndGet();

                            Long start2 = selfDelTimesPerMsg.remove(minimumMsgID.getId());
                            if(start2 != null) {
                                long time = System.nanoTime() - start2;
                                selfDeliverTime.addAndGet(time);
                            }

                            start2 = selfDelWaitQueueTimesPerMsg.remove(minimumMsgID.getId());
                            if(start2 != null) {
                                long time = System.nanoTime() - start2;
                                selfDeliverWaitingQueueTime.addAndGet(time);
                                if(log.isInfoEnabled()) {
                                    log.info("waiting time for message " + minimumMsgID + " is " + time);
                                }
                            }
                        }

                        deliver(toDeliver);
                    } else {
                        if(trace) {
                            log.trace("message is not ready to deliver " + minimumMsgID +
                                    ". State=" + minimum.state2String());
                        }
                        break;
                    }
                }
            } while(true) ;
        } finally {
            deliverLock.unlock();
            deliverMsgTime.addAndGet(System.nanoTime() - start);
            numTriesDeliver.incrementAndGet();
        }
    }*/

    //version 3
    private void deliverAllPossibleMessages_v3(long order, MessageID messageID) {
        deliver.execute(new DeliverMessage());
        /*long start = System.nanoTime();
        long startApp = System.nanoTime();
        boolean trace = log.isTraceEnabled();

        try {
            deliverLock.lock();
            start = System.nanoTime();
            if(trace) {
                log.trace("try deliver all possible messages");
            }

            MessageID minimumMsgID;

            while((minimumMsgID = dm.getNextToDeliver()) != null) {
                ReceivedMessageData minimum = received.remove(minimumMsgID);
                synchronized (minimum) {
                    if(log.isInfoEnabled()) {
                        log.info("State of the top message: " + minimum.state2String());
                    }

                    Message toDeliver = minimum.getMessage();
                    toDeliver.setSrc(minimumMsgID.getAddress());

                    if(trace) {
                        log.trace("deliver message " + minimumMsgID + " with timstamp " + minimum.getOrder());
                    }

                    //stats only!!
                    if(minimumMsgID.getAddress().equals(local_addr)) {
                        selfDeliverMsgs.incrementAndGet();

                        Long start2 = selfDelTimesPerMsg.remove(minimumMsgID.getId());
                        if(start2 != null) {
                            long time = System.nanoTime() - start2;
                            selfDeliverTime.addAndGet(time);
                        }

                        start2 = selfDelWaitQueueTimesPerMsg.remove(minimumMsgID.getId());
                        if(start2 != null) {
                            long time = System.nanoTime() - start2;
                            selfDeliverWaitingQueueTime.addAndGet(time);
                            if(log.isInfoEnabled()) {
                                log.info("waiting time for message " + minimumMsgID + " is " + time);
                            }
                        }
                    }

                    startApp = System.nanoTime();
                    deliver(toDeliver);
                    appTimeProcessingMsg.addAndGet(System.nanoTime() - startApp);
                }
            }
        } finally {
            deliverLock.unlock();
            deliverMsgTime.addAndGet(System.nanoTime() - start);
            numTriesDeliver.incrementAndGet();
        }*/
    }

    /* ------------------- METHODS FOR SEND AND DELIVER MESSAGES ------------------- */

    private void multicast(Message msg, Collection<Address> destination, boolean loopback) {
        sender.execute(new MulticastMessage(msg, new HashSet<Address>(destination), loopback));
        /*long start = System.nanoTime();
        boolean trace = log.isTraceEnabled();
        try {
            if(msg.getSrc() == null) {
                msg.setSrc(local_addr);
            }

            GroupMulticastHeader hdr = (GroupMulticastHeader) msg.getHeader(this.id);
            if(trace) {
                log.trace("try send message in multicast for " + destination + (hdr != null ? ",msg_id=" + hdr.getID() : ""));
            }

            if(!loopback) {
                destination.remove(local_addr);
            }
            destination.retainAll(actualView.get().getMembers());

            //debug only
            //msg.setFlag(Message.OOB);

            boolean mustLock = hdr != null;

            try {
                if(mustLock) {
                    seqNrLock.lock();
                    hdr.setSeqNo(seqNr++);
                }

                if(trace) {
                    log.trace("send message in multicast for this addresses=" + destination + (hdr != null ?
                            ",msg_id=" + hdr.getID() + ",seq_no=" + hdr.getSeqNo(): ""));
                }

                for(Address addr : destination) {
                    Message copy = msg.copy();
                    copy.setDest(addr);
                    down_prot.down(new Event(Event.MSG, copy));
                    sentUnicastMsgs.incrementAndGet();
                }

                if(log.isInfoEnabled()) {
                    log.info("sent message in multicast for " + destination + (hdr != null ?
                            ",msg_id=" + hdr.getID() : ""));
                }
            } finally {
                if(mustLock) {
                    seqNrLock.unlock();
                }
            }
        } finally {
            multicastTime.addAndGet(System.nanoTime() - start);
            sentGroupMsgs.incrementAndGet();
        }*/
    }

    private void deliver(Message msg) {
        Address sender=msg.getSrc();
        GroupMulticastHeader hdr = (GroupMulticastHeader) msg.getHeader(this.id);
        if(sender == null) {
            if(log.isErrorEnabled()) {
                log.error("sender is null,cannot deliver msg " + msg);
            }
            return;
        }

        if(log.isTraceEnabled()) {
            log.trace("deliver message " + (hdr != null ? "msg_id=" + hdr.getID() : msg));
        }

        deliverMsgs.incrementAndGet();
        up_prot.up(new Event(Event.MSG, msg));
    }

    //-------------------- received messages data -------------------------- //

    public class ReceivedMessageData {
        private long order;
        private Set<Address> destinations;
        private Set<Address> memberVotesInMiss; //contains the list of members that "I" never received the timestamp
        private Message message; //message to deliver

        public ReceivedMessageData() {
            order = -1;
            destinations = new HashSet<Address>();
            memberVotesInMiss = new HashSet<Address>();
        }

        public ReceivedMessageData(Collection<Address> dests, Message message) {
            this();
            destinations.addAll(dests);
            memberVotesInMiss.addAll(dests);
            this.message = message;
        }

        public void addPropose(Address from, long ts) {
            if(!memberVotesInMiss.contains(from)) {
                if(log.isWarnEnabled()) {
                    log.warn("received a strange TS propose. the member " + from + " isn't a member of " + memberVotesInMiss +
                            ". All destination members are " + destinations);
                }
                return;
            }
            memberVotesInMiss.remove(from);
            if(order == -1 || order < ts) {
                order = ts;
            }
        }

        public void putDestinationsIfAbsent(Collection<Address> destinations) {
            if(this.destinations.isEmpty()) {
                this.destinations.addAll(destinations);
                memberVotesInMiss.addAll(destinations);
            }
        }

        public void putMessageIfAbsent(Message msg) {
            if(message == null) {
                message = msg;
            }
        }

        public synchronized long getOrder() {
            return order;
        }

        public boolean canDeliver() {
            return message != null && memberVotesInMiss.isEmpty();
        }

        public Message getMessage() {
            return message;
        }

        //debug only
        public String state2String() {
            StringBuilder sb = new StringBuilder("[destinations=");
            sb.append(destinations)
                    .append(",missed members=").append(memberVotesInMiss)
                    .append(",order=").append(order)
                    .append(",has message=").append(message != null).append("]");
            return sb.toString();
        }
    }

    private class MulticastMessage implements Runnable {

        private Message msg;
        private Set<Address> destination;
        private boolean loopback;

        public MulticastMessage(Message msg, Set<Address> destination, boolean loopback) {
            this.msg = msg;
            this.destination = destination;
            this.loopback = loopback;
        }


        @Override
        public void run() {
            long start = System.nanoTime();
            boolean trace = log.isTraceEnabled();
            try {
                if(msg.getSrc() == null) {
                    msg.setSrc(local_addr);
                }

                GroupMulticastHeader hdr = (GroupMulticastHeader) msg.getHeader(id);
                if(trace) {
                    log.trace("try send message in multicast for " + destination + (hdr != null ? ",msg_id=" + hdr.getID() : ""));
                }

                if(!loopback) {
                    destination.remove(local_addr);
                }
                destination.retainAll(actualView.get().getMembers());

                //debug only
                //msg.setFlag(Message.OOB);


                if(hdr != null) {
                    hdr.setSeqNo(seqNr++);
                }

                if(trace) {
                    log.trace("send message in multicast for this addresses=" + destination + (hdr != null ?
                            ",msg_id=" + hdr.getID() + ",seq_no=" + hdr.getSeqNo(): ""));
                }

                for(Address addr : destination) {
                    Message copy = msg.copy();
                    copy.setDest(addr);
                    down_prot.down(new Event(Event.MSG, copy));
                    sentUnicastMsgs.incrementAndGet();
                }

                if(log.isInfoEnabled()) {
                    log.info("sent message in multicast for " + destination + (hdr != null ?
                            ",msg_id=" + hdr.getID() : ""));
                }
            } finally {
                multicastTime.addAndGet(System.nanoTime() - start);
                sentGroupMsgs.incrementAndGet();
            }
        }
    }

    private class DeliverMessage implements Runnable {

        @Override
        public void run() {
            long start = System.nanoTime();
            boolean trace = log.isTraceEnabled();
            try {
                if(trace) {
                    log.trace("try deliver all possible messages");
                }

                MessageID minimumMsgID;

                while((minimumMsgID = dm.getNextToDeliver()) != null) {
                    ReceivedMessageData minimum = received.remove(minimumMsgID);
                    synchronized (minimum) {
                        if(log.isInfoEnabled()) {
                            log.info("State of the top message: " + minimum.state2String());
                        }

                        Message toDeliver = minimum.getMessage();
                        toDeliver.setSrc(minimumMsgID.getAddress());

                        if(trace) {
                            log.trace("deliver message " + minimumMsgID + " with timstamp " + minimum.getOrder());
                        }

                        //stats only!!
                        if(minimumMsgID.getAddress().equals(local_addr)) {
                            selfDeliverMsgs.incrementAndGet();

                            Long start2 = selfDelTimesPerMsg.remove(minimumMsgID.getId());
                            if(start2 != null) {
                                long time = System.nanoTime() - start2;
                                selfDeliverTime.addAndGet(time);
                            }

                            start2 = selfDelWaitQueueTimesPerMsg.remove(minimumMsgID.getId());
                            if(start2 != null) {
                                long time = System.nanoTime() - start2;
                                selfDeliverWaitingQueueTime.addAndGet(time);
                                if(log.isInfoEnabled()) {
                                    log.info("waiting time for message " + minimumMsgID + " is " + time);
                                }
                            }
                        }

                        long startApp = System.nanoTime();
                        deliver(toDeliver);
                        appTimeProcessingMsg.addAndGet(System.nanoTime() - startApp);
                    }
                }
            } finally {
                deliverMsgTime.addAndGet(System.nanoTime() - start);
                numTriesDeliver.incrementAndGet();
            }
        }
    }
}
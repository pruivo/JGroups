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
import org.jgroups.groups.failure.FailureManager;
import org.jgroups.groups.failure.FinalMessagesManager;
import org.jgroups.stack.Protocol;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Pedro
 */
@MBean(objectName="GroupMulticast", description="Provides atomic multicast properties")
public class GROUP_MULTICAST extends Protocol {

    //ArrayBlockingQueue needs a fixed capacity!
    private static final int MAX_QUEUE_SIZE = 65000;

    //stats
    private final AtomicLong newMessages = new AtomicLong(0);
    private final AtomicLong proposeMessages = new AtomicLong(0);
    private final AtomicLong finalMessages = new AtomicLong(0);
    private final AtomicLong sentGroupMessages = new AtomicLong(0);
    private final AtomicLong sentUnicastMessages = new AtomicLong(0);
    private final AtomicLong deliveredMessages = new AtomicLong(0);

    //measure times
    private final AtomicLong multicastTime = new AtomicLong(0);
    private final AtomicLong unicastTime = new AtomicLong(0);
    private final AtomicLong newMsgTime = new AtomicLong(0);
    private final AtomicLong proposeMsgTime = new AtomicLong(0);
    private final AtomicLong finalMsgTime = new AtomicLong(0);
    private final AtomicLong deliverMsgTime = new AtomicLong(0);
    private final AtomicLong numTriesDeliver = new AtomicLong(0);

    //self deliver stats
    private final AtomicLong selfDeliverTime = new AtomicLong(0);
    private final AtomicLong selfDeliverWaitingQueueTime = new AtomicLong(0);
    private final AtomicLong selfDeliverMessages = new AtomicLong(0);
    private final AtomicLong appTimeProcessingMsg = new AtomicLong(0);  //time that the application takes to process a message
    private final Map<Long,Long> selfDelTimePerMsg =new ConcurrentHashMap<Long,Long>();
    private final Map<Long,Long> selfDelWaitQueueTimePerMsg =new ConcurrentHashMap<Long,Long>();

    //id of messages and clock for timestamp the messages
    private final ReentrantLock clockLock = new ReentrantLock();
    private long clock = 0;
    private final AtomicLong message_id = new AtomicLong(0);

    //for view change
    private final AtomicReference<View> actualView = new AtomicReference<View>();
    private final FinalMessagesManager finalMsgMan = new FinalMessagesManager();
    private final FailureManager failureManager = new FailureManager();

    //node info
    private Address local_addr = null;
    private final AtomicReference<Address> coordinator = new AtomicReference<Address>(null);
    private ExecutorService sender = Executors.newSingleThreadExecutor();

    //received and sended messages information
    private final ConcurrentMap<MessageID, ReceiverInfo> received = new ConcurrentHashMap<MessageID, ReceiverInfo>();
    private final ConcurrentMap<MessageID, SenderInfo> sended = new ConcurrentHashMap<MessageID, SenderInfo>();

    //to deliver messages
    private final DeliverThread deliver = new DeliverThread();
    private final DeliverManager dm = new DeliverManager();

    //log
    private boolean trace = false;
    private boolean error = false;
    private boolean debug = false;
    private boolean warn = false;

    @Override
    public void start() throws Exception {
        super.start();
        deliver.start();
        trace = log.isTraceEnabled();
        error = log.isErrorEnabled();
        debug = log.isDebugEnabled();
        warn = log.isWarnEnabled();
        finalMsgMan.start();
        resetStats();
        if(sender.isShutdown() || sender.isTerminated()) {
            sender = Executors.newSingleThreadExecutor();
        }
    }

    @Override
    public void stop() {
        super.stop();
        sender.shutdown();
        received.clear();
        sended.clear();
        finalMsgMan.stop();
        resetStats();
        dm.clear();
        deliver.interrupt();
        sender.shutdownNow();
    }

    @ManagedOperation
    public void resetStats() {
        newMessages.set(0);
        proposeMessages.set(0);
        finalMessages.set(0);
        deliveredMessages.set(0);
        sentGroupMessages.set(0);
        sentUnicastMessages.set(0);
        multicastTime.set(0);
        unicastTime.set(0);
        newMsgTime.set(0);
        proposeMsgTime.set(0);
        finalMsgTime.set(0);
        deliverMsgTime.set(0);
        selfDeliverTime.set(0);
        selfDeliverMessages.set(0);
        selfDelTimePerMsg.clear();
        numTriesDeliver.set(0);
        appTimeProcessingMsg.set(0);
    }

    @ManagedAttribute
    public long getReceivedMessages() {
        return newMessages.get() + proposeMessages.get() + finalMessages.get();
    }

    @ManagedAttribute
    public long getDeliveredMessages() {
        return deliveredMessages.get();
    }

    @ManagedAttribute
    public long getProposedMessages() {
        return proposeMessages.get();
    }

    @ManagedAttribute
    public long getNewMessages() {
        return newMessages.get();
    }

    @ManagedAttribute
    public long getFinaldMessages() {
        return finalMessages.get();
    }

    @ManagedAttribute
    public long getSentGroupMessages() {
        return sentGroupMessages.get();
    }

    @ManagedAttribute
    public long getSentUnicastMessages() {
        return sentUnicastMessages.get();
    }

    @ManagedAttribute
    public long getUnicastTime() {
        return unicastTime.get();
    }

    @ManagedAttribute
    public long getMulticastTime() {
        return multicastTime.get();
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
    public long getSelfDeliveredMessages() {
        return selfDeliverMessages.get();
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
        m.put("received_messages", getReceivedMessages());
        m.put("delivered_messages", deliveredMessages.get());
        m.put("proposed_messages", proposeMessages.get());
        m.put("sent_group_messages", sentGroupMessages.get());
        m.put("sent_unicast_messages", sentUnicastMessages.get());
        m.put("multicast_time", multicastTime.get());
        m.put("unicast_time", unicastTime.get());
        m.put("new_msg_time", newMsgTime.get());
        m.put("propose_msg_time", proposeMsgTime.get());
        m.put("final_msg_time", finalMsgTime.get());
        m.put("deliver_msg_time", deliverMsgTime.get());
        m.put("self_deliver_time", selfDeliverTime.get());
        m.put("self_deliver_waiting_time", selfDeliverWaitingQueueTime.get());
        m.put("self_delivered_messages", selfDeliverMessages.get());
        m.put("number_tries_deliver",numTriesDeliver.get());
        m.put("app_time_processing", appTimeProcessingMsg.get());
        return m;
    }

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address addr = msg.getDest();
                if(addr != null && addr instanceof GroupAddress) {
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

                        if(destination.contains(local_addr)) {
                            selfDelTimePerMsg.put(msgID.getId(), System.nanoTime());
                        }

                        if(log.isInfoEnabled()) {
                            log.info("send multicast message. message_id=" + msgID + " to " + destination);
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
                    case GroupMulticastHeader.MESSAGE_PROPOSE:
                        processProposeMsg(msg, hdr);
                        return null;
                    case GroupMulticastHeader.MESSAGE_FINAL:
                        processFinalMsg(msg, hdr);
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
        Address failedNode = hdr.getOrigin();
        Map<Long, Long> deliveredMsgs = (Map<Long, Long>) msg.getObject();

        if(trace) {
            log.trace("Received message to order from=" + msg.getSrc() +
                    ",for the failed member=" + failedNode);
        }

        failureManager.processMember(msg.getSrc(), failedNode, deliveredMsgs);
        if(failureManager.isOK(failedNode)) {
            if(trace) {
                log.trace("Messages from failed node " + failedNode + " are ordered. response to order nodes");
            }
            Map<Long, Long> res = failureManager.getFinalTimestamp(failedNode);
            failureManager.removeFailedMember(failedNode);
            Message response = new Message();
            response.setObject(res);

            GroupMulticastHeader hdr2 = new GroupMulticastHeader(failedNode);
            hdr2.setType(GroupMulticastHeader.COORDINATOR_RESPONSE);
            response.putHeader(this.id, hdr2);

            unicast(response, false);
        }
    }

    //message from coordinator with the new order
    private void processMessages(Message msg, GroupMulticastHeader hdr) {
        Address failedNode = hdr.getOrigin();
        Map<Long, Long> finalTs = (Map<Long, Long>) msg.getObject();

        if(trace) {
            log.trace("Received a message from coordinator with the order for failed member=" + failedNode);
        }

        synchronized (received) {
            Set<MessageID> toRemove = new HashSet<MessageID>();
            for(Map.Entry<MessageID, ReceiverInfo> entry : received.entrySet()) {
                MessageID msgID = entry.getKey();

                if(msgID.getAddress().equals(failedNode)) {
                    Long ts = finalTs.get(msgID.getId());
                    if(ts != null) {
                        entry.getValue().setFinalTimestamp(ts);
                    } else {
                        toRemove.add(msgID);
                    }
                }
            }
            received.keySet().removeAll(toRemove);
        }
        failureManager.removeFailedNode(failedNode);
        finalMsgMan.deleteAddress(failedNode);
        deliverAllPossibleMessages();
    }

    private void handleViewChange(View oldView, View newView) {
        /*if(trace) {
            log.trace("handle view change. old_view=" + (oldView != null ? oldView.getViewId() : "null") + ",new_view=" + newView.getViewId());
        }

        //join member... do nothing
        //the first time is null...
        if(oldView == null || oldView.size() < newView.size()) {
            if(trace) {
                log.trace("It is a joining member... ignore");
            }
            return;
        }

        if(trace) {
            log.trace("It is a leaving member... it is necessary to send messages in hold to coordinator for ordering");
        }

        Set<Address> failedNodes = new HashSet<Address>(oldView.getMembers());
        failedNodes.removeAll(newView.getMembers()); //the failed nodes

        Address prevCoord;
        Address newCoord = newView.getMembers().iterator().next();
        do {
            prevCoord = coordinator.get();
        } while(!coordinator.compareAndSet(prevCoord, newCoord));

        if(!newCoord.equals(prevCoord)) {
            if(trace) {
                log.trace("it was the failure of coordinator... send all messages waiting response from old coordinator");
            }
            failedNodes.addAll(failureManager.getFailedNodes());
        }

        failureManager.addAllFailedNode(failedNodes);
        if(amICoordinator()) {
            failureManager.handleFailure(failedNodes, newView.getMembers());
        }

        for(Address failedNode : failedNodes) {
            Map<Long, Long> deliver = new HashMap<Long, Long>();

            for(Map.Entry<Long, Long> entry : finalMsgMan.getFinalMessagesFrom(failedNode).entrySet()) {
                deliver.put(entry.getKey(), entry.getValue());
            }

            Message toCoord = new Message(getCoordinator());
            GroupMulticastHeader hdr = new GroupMulticastHeader(failedNode);
            hdr.setType(GroupMulticastHeader.COORDINATOR_REQUEST);

            toCoord.putHeader(this.id, hdr);
            toCoord.setObject(deliver);

            unicast(toCoord, false);
        }

        Set<MessageID> toRemove = new HashSet<MessageID>();
        for(Map.Entry<MessageID, SenderInfo> entry : sended.entrySet()) {
            SenderInfo sendInfo = entry.getValue();
            MessageID msgID = entry.getKey();

            sendInfo.handleFailure(failedNodes);

            if(sendInfo.isFinal()) {
                toRemove.add(msgID);
                long finalTs = sendInfo.getGreatestTimestampReceived();

                GroupMulticastHeader newHdr = new GroupMulticastHeader(local_addr, msgID.getId());
                newHdr.setType(GroupMulticastHeader.MESSAGE_FINAL);
                newHdr.setTimestamp(finalTs);

                Message msgFinal = new Message(null);
                msgFinal.setSrc(local_addr);
                msgFinal.putHeader(this.id, newHdr);

                multicast(msgFinal, sendInfo.getDestination(), false);
            }
        }
        sended.keySet().removeAll(toRemove);*/
    }

    private void processNewMsg(Message msg, GroupMulticastHeader hdr) {
        //log.warn("new");
        long start = System.nanoTime();
        try {
            MessageID msgID = hdr.getID();
            Address from = msg.getSrc();
            Address origin = hdr.getOrigin();

            if(log.isInfoEnabled()) {
                log.info("incoming new message. msg_id=" + msgID + ";from=" + from + ";origin=" + origin);
            }

            if(!from.equals(origin)) {
                if(warn) {
                    log.warn("received new message but it wasn't send by the original sender. discart it. msg_id=" + msgID);
                }
                return;
            }

            ReceiverInfo msgInfo = new ReceiverInfo(msg);
            ReceiverInfo existing = received.putIfAbsent(msgID, msgInfo);
            if(existing != null) {
                if(warn) {
                    log.warn("received new message but the message already exists. discart it. msg_id=" + msgID);
                }
            } else {
                long ts = 0;
                try {
                    clockLock.lock();
                    clock = clock + 1;
                    ts = clock;
                    dm.update(msgID, -1, ts);
                } finally {
                    clockLock.unlock();
                }
                msgInfo.setTemporaryTimestamp(ts);

                GroupMulticastHeader newHdr = hdr.copy();
                newHdr.setType(GroupMulticastHeader.MESSAGE_PROPOSE);
                newHdr.setTimestamp(ts);

                Message newMsg = new Message(origin, local_addr, null);
                newMsg.putHeader(this.id, newHdr);

                if(log.isInfoEnabled()) {
                    log.info("send timestamp propose msg_id=" + msgID + ",to=" + origin + ",ts=" + ts);
                }

                unicast(newMsg, false);
            }
        } finally {
            newMsgTime.addAndGet(System.nanoTime() - start);
            newMessages.incrementAndGet();
        }
    }

    private void processProposeMsg(Message msg, GroupMulticastHeader hdr) {
        //log.warn("propose");
        long start = System.nanoTime();
        try {
            MessageID msgID = hdr.getID();
            Address from = msg.getSrc();
            Address origin = hdr.getOrigin();

            if(log.isInfoEnabled()) {
                log.info("incoming propose timestamp message. msg_id=" + msgID + ";from=" + from + ";origin=" + origin + ",ts=" + hdr.getTimestamp());
            }

            if(!local_addr.equals(origin)) {
                if(warn) {
                    log.warn("received a propose timestamp message, but it wasn't send by me! discart it. msg_id=" + msgID + ",origin=" + origin);
                }
                return;
            }

            long proposedTs = hdr.getTimestamp();

            try {
                clockLock.lock();
                if(clock < proposedTs) {
                    clock = proposedTs;
                }
            } finally {
                clockLock.unlock();
            }

            SenderInfo sendInfo = sended.get(msgID);
            if(sendInfo != null) {
                sendInfo.addTimestamp(proposedTs, from);
                if(sendInfo.isFinal()) {
                    sended.remove(msgID);
                    long finalTs = sendInfo.getGreatestTimestampReceived();

                    GroupMulticastHeader newHdr = hdr.copy();
                    newHdr.setType(GroupMulticastHeader.MESSAGE_FINAL);
                    newHdr.setTimestamp(finalTs);

                    Message msgFinal = new Message(null);
                    msgFinal.setSrc(local_addr);
                    msgFinal.putHeader(this.id, newHdr);

                    if(trace) {
                        log.trace("all proposes received. send final timestamp. msg_id=" + msgID + ",ts=" + finalTs);
                    }

                    multicast(msgFinal, sendInfo.getDestination(), false);
                } else {
                    if(trace) {
                        log.trace("proposes in misses. waiting all proposes. msg_id=" + msgID);
                    }
                }

            } else {
                if(warn) {
                    log.warn("message originate from me not found in sended messages. discart it. msg_id=" + msgID);
                }
            }
        } finally {
            proposeMsgTime.addAndGet(System.nanoTime() - start);
            proposeMessages.incrementAndGet();
        }
    }

    private void processFinalMsg(Message msg, GroupMulticastHeader hdr) {
        //log.warn("final");
        long start = System.nanoTime();
        try {
            MessageID msgID = hdr.getID();
            Address from = msg.getSrc();
            Address origin = hdr.getOrigin();

            if(log.isInfoEnabled()) {
                log.info("incoming final timestamp message. msg_id=" + msgID + ";from=" + from + ";origin=" + origin + ",ts=" + hdr.getTimestamp() + ",queue size=" + received.size());
            }

            if(!from.equals(origin)) {
                if(warn) {
                    log.warn("received a final timestamp message from a node that it is not the sender. discart it. msg_id=" + msgID + ",sender=" + from + ",origin=" + origin);
                }
                return ;
            }

            ReceiverInfo existing = received.get(msgID);
            if(existing != null) {
                long finalTs = hdr.getTimestamp();
                long oldTs = existing.getTimestamp();

                try {
                    clockLock.lock();
                    if(clock < finalTs) {
                        clock = finalTs;
                    }
                    dm.updateAndMarkCompleted(msgID, oldTs, finalTs);
                } finally {
                    clockLock.unlock();
                }

                existing.setFinalTimestamp(finalTs);
                finalMsgMan.setFinalTs(msgID, finalTs);

                if(msgID.getAddress().equals(local_addr)) {
                    selfDelWaitQueueTimePerMsg.put(msgID.getId(), System.nanoTime());
                }

                deliverAllPossibleMessages();

            } else {
                if(warn) {
                    log.warn("received a final timestamp message, but I never received the original message. discart it. msg_id=" + msgID);
                }
            }
        } finally {
            finalMsgTime.addAndGet(System.nanoTime() - start);
            finalMessages.incrementAndGet();
        }
    }

    //version 2
    private void deliverAllPossibleMessages() {
        deliver.notifyNewMessage();
        //log.warn("deliver all possible messages");
        /*long start = System.nanoTime();
        try {
            deliverLock.lock();

            if(trace) {
                log.trace("try deliver all possible messages");
            }

            do {
                long minimumTs = Long.MAX_VALUE;
                MessageID minimumMsgID = null;
                for(Map.Entry<MessageID, ReceiverInfo> entry : received.entrySet()) {
                    ReceiverInfo ri = entry.getValue();
                    MessageID msgID = entry.getKey();
                    long ts = ri.getTimestamp();

                    if(ts < minimumTs || (ts == minimumTs && msgID.compareTo(minimumMsgID) < 0)) {
                        minimumTs = ts;
                        minimumMsgID = msgID;
                    }
                }

                if(minimumMsgID == null) {
                    break;
                }

                ReceiverInfo minimum = received.get(minimumMsgID);
                if(minimum.isFinal() && minimumTs == minimum.getTimestamp()) {
                    received.remove(minimumMsgID);

                    Message toDeliver = minimum.getMessage();
                    toDeliver.setSrc(minimum.getOrigin());

                    if(trace) {
                        log.trace("put in deliver queue message " + minimumMsgID + " with timstamp " + minimumTs);
                    }

                    if(minimumMsgID.getAddress().equals(local_addr)) {
                        selfDeliverMessages.incrementAndGet();

                        Long start2 = selfDelTimePerMsg.remove(minimumMsgID.getId());
                        long time1 = 0, time2 = 0;
                        if(start2 != null) {
                            time1 = System.nanoTime() - start2;
                            selfDeliverTime.addAndGet(time1);
                        }

                        start2 = selfDelWaitQueueTimePerMsg.remove(minimumMsgID.getId());
                        if(start2 != null) {
                            time2 = System.nanoTime() - start2;
                            selfDeliverWaitingQueueTime.addAndGet(time2);
                        }
                        if(log.isInfoEnabled()) {
                            log.info("deliver message " + minimumMsgID + ",self_deliver_time=" + time1 +
                                    ",waiting time=" + time2);
                        }
                    }

                    deliver.addMessage(toDeliver, minimumMsgID);
                } else {
                    break;
                }
            } while(true) ;
        } finally {
            deliverLock.unlock();
            deliverMsgTime.addAndGet(System.nanoTime() - start);
            numTriesDeliver.incrementAndGet();
        }*/
    }

    /*private void deliverAllPossibleMessages() {
        long start = System.nanoTime();
        try {
            deliverLock.lock();
            if(trace) {
                log.trace("try deliver all possible messages");
            }
            TreeSet<ReceiverInfo> sortedMessages = new TreeSet<ReceiverInfo>(received.values());

            if(debug) {
                log.debug("messages list=" + sortedMessages);
            }

            do {
                if(sortedMessages.isEmpty()) {
                    //System.out.println("no messages left");
                    break;
                }
                ReceiverInfo first = sortedMessages.pollFirst();
                if(first.isFinal()) {
                    //double check
                    TreeSet<ReceiverInfo> doubleCheck = new TreeSet<ReceiverInfo>(received.values());

                    ReceiverInfo ri = doubleCheck.pollFirst();
                    if(first != ri) { //must be the same reference!

                        if(debug) {
                            log.debug("double check fail for message " + first.getMessageID());
                        }
                        break; //another thread will continue this job (i think)
                    }

                    MessageID msgID = first.getMessageID();
                    received.remove(msgID);

                    Message toDeliver = first.getMessage();
                    toDeliver.setSrc(first.getOrigin());

                    if(trace) {
                        log.trace("put in deliver queue message " + msgID + " with timstamp " + first.getTimestamp());
                    }

                    deliver.addMessage(toDeliver, msgID);
                } else {
                    //System.out.println("wait for: " + first);
                    break;
                }
            } while(true);
        } finally {
            deliverLock.unlock();
            deliverMsgTime.addAndGet(System.nanoTime() - start);
        }
    }*/

    /* ------------------- METHODS FOR SEND AND DELIVER MESSAGES ------------------- */

    private void multicast(Message msg, Collection<Address> destination, boolean fifo) {
        sender.execute(new MulticastMessage(msg, new HashSet<Address>(destination)));
        /*long start = System.nanoTime();
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

            boolean loopback = destination.remove(local_addr);

            for(Address addr : destination) {
                Message copy = msg.copy();
                copy.setDest(addr);
                unicast(copy, false); //already have no fifo flag
            }

            if(loopback) {
                Message copy = msg.copy();
                copy.setDest(local_addr);
                up(new Event(Event.MSG, copy));
            }
        } finally {
            multicastTime.addAndGet(System.nanoTime() - start);
        }
        sentGroupMessages.incrementAndGet();*/
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

            GroupMulticastHeader hdr = (GroupMulticastHeader) msg.getHeader(this.id);
            if(trace) {
                log.trace("send message in unicast. msg=" + msg  + (hdr != null ? ",msg_id=" + hdr.getID() : ""));
            }
            down_prot.down(new Event(Event.MSG, msg));
        } finally {
            unicastTime.addAndGet(System.nanoTime() - start);
        }
        sentUnicastMessages.incrementAndGet();
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

        if(log.isInfoEnabled()) {
            log.info("deliver message "+ (hdr != null ? ",msg_id=" + hdr.getID() : msg));
        }

        deliveredMessages.incrementAndGet();
        up_prot.up(new Event(Event.MSG, msg));
    }


/* ----------------------------------- DELIVER THREAD ---------------------------------------------*/

    public class DeliverThread extends Thread {
        private final AtomicBoolean accept = new AtomicBoolean(true);
        private final Semaphore empty = new Semaphore(0);

        @Override
        public void run() {
            MessageID minimumMsgID = null;
            while(!isInterrupted()) {
                try {
                    empty.acquire();
                } catch (InterruptedException e) {
                    continue;
                }

                long start = System.nanoTime();
                boolean trace = log.isTraceEnabled();
                try {
                    if(trace) {
                        log.trace("try deliver all possible messages");
                    }

                    while((minimumMsgID = dm.getNextToDeliver()) != null) {
                        ReceiverInfo minimum = received.remove(minimumMsgID);

                        Message toDeliver = minimum.getMessage();
                        toDeliver.setSrc(minimum.getOrigin());

                        if(trace) {
                            log.trace("message to deliver: " + minimumMsgID + " with timstamp " + minimum.getTimestamp());
                        }

                        if(minimumMsgID.getAddress().equals(local_addr)) {
                            selfDeliverMessages.incrementAndGet();

                            Long start2 = selfDelTimePerMsg.remove(minimumMsgID.getId());
                            long time1 = 0, time2 = 0;
                            if(start2 != null) {
                                time1 = System.nanoTime() - start2;
                                selfDeliverTime.addAndGet(time1);
                            }

                            start2 = selfDelWaitQueueTimePerMsg.remove(minimumMsgID.getId());
                            if(start2 != null) {
                                time2 = System.nanoTime() - start2;
                                selfDeliverWaitingQueueTime.addAndGet(time2);
                            }
                            if(log.isInfoEnabled()) {
                                log.info("deliver message " + minimumMsgID + ",self_deliver_time=" + time1 +
                                        ",waiting time=" + time2);
                            }
                        }

                        long procStart = System.nanoTime();
                        deliver(toDeliver);
                        appTimeProcessingMsg.addAndGet(System.nanoTime() - procStart);
                    }
                } catch(Exception e) {
                    if(warn) {
                        log.warn("Exception caught on deliver thread for message:" + minimumMsgID + ",exception:" + e);
                    }
                    e.printStackTrace();
                } finally {
                    deliverMsgTime.addAndGet(System.nanoTime() - start);
                    numTriesDeliver.incrementAndGet();
                }
            }
        }

        @Override
        public void start() {
            accept.set(true);
            super.start();
        }

        @Override
        public void interrupt() {
            accept.set(false);
            super.interrupt();
            empty.drainPermits();
        }

        public void notifyNewMessage() {
            if(accept.get()) {
                empty.release();
            }
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
            this.destsLeft = new ConcurrentSkipListSet<Address>(destination);
            this.destination = new HashSet<Address>(destination);
        }

        public long getGreatestTimestampReceived() {
            return greatestTimestampReceived;
        }

        public synchronized void addTimestamp(long ts, Address from) {
            if(ts < 0) {
                return;
            }

            if(destsLeft.remove(from)) {
                long oldValue = greatestTimestampReceived;
                if(oldValue < ts) {
                    greatestTimestampReceived = ts;
                }
            }
        }

        public synchronized boolean isFinal() {
            return destsLeft.isEmpty();
        }

        public Set<Address> getDestination() {
            return destination;
        }

        /*public synchronized void handleFailure(Collection<Address> failedNodes) {
            destsLeft.removeAll(failedNodes);
            destination.removeAll(failedNodes);
        }*/
    }

    private class MulticastMessage implements Runnable {

        private Message msg;
        private Set<Address> destination;

        public MulticastMessage(Message msg, Set<Address> destination) {
            this.msg = msg;
            this.destination = destination;
        }


        @Override
        public void run() {
            long start = System.nanoTime();
            try {
                if(msg.getSrc() == null) {
                    msg.setSrc(local_addr);
                }

                GroupMulticastHeader hdr = (GroupMulticastHeader) msg.getHeader(id);
                if(trace) {
                    log.trace("send message in multicast. msg=" + msg + ",destination=" + destination + (hdr != null ? ",msg_id=" + hdr.getID() : ""));
                }

                //boolean loopback = destination.remove(local_addr);

                for(Address addr : destination) {
                    Message copy = msg.copy();
                    copy.setDest(addr);
                    unicast(copy, true); //already have no fifo flag
                }
            } finally {
                multicastTime.addAndGet(System.nanoTime() - start);
                sentGroupMessages.incrementAndGet();
            }
        }
    }
}
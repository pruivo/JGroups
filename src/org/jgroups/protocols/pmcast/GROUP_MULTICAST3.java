package org.jgroups.protocols.pmcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.groups.GroupAddress;
import org.jgroups.groups.GroupMulticastHeader;
import org.jgroups.groups.MessageID;
import org.jgroups.stack.Protocol;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Pedro
 */
public class GROUP_MULTICAST3 extends Protocol {

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
    private final AtomicLong message_id = new AtomicLong(0);

    //for view change
    private final AtomicReference<View> actualView = new AtomicReference<View>();
    //private final FailureManager failureManager = new FailureManager();

    //node info
    private Address local_addr = null;

    //log
    private boolean trace = false;
    //private boolean error = false;
    private boolean debug = false;
    //private boolean warn = false;

    @Override
    public void start() throws Exception {
        super.start();
        trace = log.isTraceEnabled();
        //error = log.isErrorEnabled();
        debug = log.isDebugEnabled();
        //warn = log.isWarnEnabled();
    }

    @Override
    public void stop() {
        super.stop();
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

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address addr = msg.getDest();
                if(addr != null && addr instanceof GroupAddress) {
                    Set<Address> destination = ((GroupAddress)addr).getAddresses();

                    GroupMulticastHeader hdr = new GroupMulticastHeader(local_addr, message_id.incrementAndGet());
                    hdr.addDestinations(destination);
                    MessageID msgID = hdr.getID();

                    msg.putHeader(this.id, hdr);

                    if(statsEnabled()) {
                        self_del_times.put(msgID.getId(), System.nanoTime());
                    }

                    return multicast(msg, destination, true);
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

                if(hdr.getDestinations().contains(local_addr)) {
                    if(statsEnabled()) {
                        long start = self_del_times.get(hdr.getID().getId());
                        self_deliver_time.addAndGet(System.nanoTime() - start);
                    }
                    return up_prot.up(evt);
                }
                return null;

            case Event.VIEW_CHANGE:
                View oldView;
                View newView = (View)evt.getArg();
                do{
                    oldView = actualView.get();
                } while(!actualView.compareAndSet(oldView, newView));
                break;
        }

        return up_prot.up(evt);
    }

    /* ------------------- METHODS FOR SEND AND DELIVER MESSAGES ------------------- */

    private Object multicast(Message msg, Collection<Address> destination, boolean fifo) {
        long start = System.nanoTime();
        try {
            if(msg.getSrc() == null) {
                msg.setSrc(local_addr);
            }
            msg.setDest(null); //broadcast

            GroupMulticastHeader hdr = (GroupMulticastHeader) msg.getHeader(this.id);
            if(trace) {
                log.trace("send message in multicast. msg=" + msg + ",destination=" + destination + (hdr != null ? ",msg_id=" + hdr.getID() : ""));
            }

            return down_prot.down(new Event(Event.MSG, msg));
        } finally {
            multicastTime.addAndGet(System.nanoTime() - start);
            sent_group_msgs++;
        }
    }
}
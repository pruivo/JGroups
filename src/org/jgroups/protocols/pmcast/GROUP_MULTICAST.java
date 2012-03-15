package org.jgroups.protocols.pmcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.groups.DeliverProtocol;
import org.jgroups.groups.GroupAddress;
import org.jgroups.groups.MessageID;
import org.jgroups.groups.header.GroupMulticastHeader;
import org.jgroups.groups.manager.DeliverManagerImpl;
import org.jgroups.groups.manager.SenderManager;
import org.jgroups.groups.manager.SequenceNumberManager;
import org.jgroups.groups.threading.DeliverThread;
import org.jgroups.groups.threading.SenderThread;
import org.jgroups.stack.Protocol;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * // TODO: Document this
 *
 * Total Order Multicast with three communication steps
 *
 * @author pruivo
 * @since 4.0
 */
@MBean(description = "Implementation of Total Order Multicast based on Skeen's Algorithm")
public class GROUP_MULTICAST extends Protocol implements DeliverProtocol {
    //managers
    private DeliverManagerImpl deliverManager;
    private SenderManager senderManager;

    //thread
    private final DeliverThread deliverThread;
    private final SenderThread multicastSenderThread;
    
    //local address
    private Address localAddress;

    //sequence numbers, messages ids and lock
    private final SequenceNumberManager sequenceNumberManager;
    private long messageIdCounter;
    private final Lock sendLock;

    public GROUP_MULTICAST() {
        deliverThread = new DeliverThread(this);
        multicastSenderThread = new SenderThread(this);
        sequenceNumberManager = new SequenceNumberManager();
        sendLock = new ReentrantLock();
        messageIdCounter = 0;
    }

    @Override
    public void start() throws Exception {
        deliverManager = new DeliverManagerImpl();
        senderManager = new SenderManager();
        deliverThread.start(deliverManager);
        multicastSenderThread.clear();
        multicastSenderThread.start();
    }

    @Override
    public void stop() {
        deliverThread.interrupt();
        multicastSenderThread.interrupt();
    }

    @Override
    public Object down(Event evt) {
        switch (evt.getType()) {
            case Event.MSG:
                handleDownMessage(evt);
                return null;
            case Event.SET_LOCAL_ADDRESS:
                this.localAddress = (Address) evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleViewChange((View) evt.getArg());
                break;
            default:
                break;
        }
        return down_prot.down(evt);
    }

    @Override
    public Object up(Event evt) {
        switch (evt.getType()) {
            case Event.MSG:
                handleUpMessage(evt);
                return null;
            case Event.VIEW_CHANGE:
                handleViewChange((View) evt.getArg());
                break;
            case Event.SET_LOCAL_ADDRESS:
                this.localAddress = (Address) evt.getArg();
                break;
            default:
                break;
        }
        return up_prot.up(evt);
    }

    private void handleViewChange(View view) {
        if (log.isTraceEnabled()) {
            log.trace("Handle view " + view);
        }
        // TODO: Customise this generated block
    }

    private void handleDownMessage(Event evt) {
        Message message = (Message) evt.getArg();
        Address dest = message.getDest();

        if (dest != null && dest instanceof GroupAddress && !message.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
            //group multicast message
            handleDownGroupMulticastMessage(message);
        } else if (dest != null && dest instanceof GroupAddress) {
            //group address with NO_TOTAL_ORDER flag (should no be possible, but...)
            handleDownGroupMessage(message);
        } else {
            //normal message
            down_prot.down(evt);
        }
    }

    private void handleDownGroupMessage(Message message) {
        if (log.isTraceEnabled()) {
            log.trace("Handle message with Group Address but with Flag NO_TOTAL_ORDER set");
        }
        GroupAddress groupAddress = (GroupAddress) message.getDest();
        try {
            multicastSenderThread.addMessage(message, groupAddress.getAddresses());
        } catch (InterruptedException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
    }

    private void handleDownGroupMulticastMessage(Message message) {
        if (log.isTraceEnabled()) {
            log.trace("Handle group multicast message");
        }
        GroupAddress groupAddress = (GroupAddress) message.getDest();
        Set<Address> destination = groupAddress.getAddresses();

        if (destination.isEmpty()) {
            if (log.isWarnEnabled()) {
                log.warn("Received a group address with an empty list");
            }
            throw new IllegalStateException("Group Address must have at least one element");
        }

        if (destination.size() == 1) {
            if (log.isWarnEnabled()) {
                log.warn("Received a group address with an element");
            }
            message.setDest(destination.iterator().next());
            down_prot.down(new Event(Event.MSG, message));
            return;
        }

        boolean localAddressInDestination = destination.contains(localAddress);

        try {
            sendLock.lock();
            MessageID messageID = new MessageID(localAddress, messageIdCounter++);
            long sequenceNumber = sequenceNumberManager.getAndIncrement();

            GroupMulticastHeader header = GroupMulticastHeader.createNewHeader(GroupMulticastHeader.MESSAGE,
                    messageID);
            header.setSequencerNumber(sequenceNumber);
            header.addDestinations(destination);
            message.putHeader(this.id, header);

            senderManager.addNewMessageToSent(messageID, destination, sequenceNumber, localAddressInDestination);

            if (log.isTraceEnabled()) {
                log.trace("Sending message " + messageID + " to " + destination + " with initial sequence number of " +
                        sequenceNumber);
            }

            if (localAddressInDestination) {
                Set<Address> destinationWithoutLocalAddress = new HashSet<Address>(destination);
                destinationWithoutLocalAddress.remove(localAddress);

                deliverManager.addNewMessageToDeliver(messageID, message, sequenceNumber);

                multicastSenderThread.addMessage(message, destinationWithoutLocalAddress);
            } else {
                multicastSenderThread.addMessage(message, destination);
            }


        } catch (InterruptedException e) {
            if (log.isWarnEnabled()) {
                log.warn("Interrupted exception received while handling group multicast message");
            }
            e.printStackTrace();  // TODO: Customise this generated block
        } finally {
            sendLock.unlock();
        }
    }

    private void handleUpMessage(Event evt) {
        Message message = (Message) evt.getArg();

        GroupMulticastHeader header = (GroupMulticastHeader) message.getHeader(this.id);

        if (header == null) {
            up_prot.up(evt);
            return;
        }

        MessageID messageID = header.getMessageID();
        try {
            switch (header.getType()) {
                case GroupMulticastHeader.MESSAGE:
                    //create the sequence number and put it in deliver manager
                    long myProposeSequenceNumber = sequenceNumberManager.updateAndGet(header.getSequencerNumber());
                    deliverManager.addNewMessageToDeliver(messageID, message, myProposeSequenceNumber);

                    if (log.isTraceEnabled()) {
                        log.trace("Received the message with " + header + ". The proposed sequence number is " +
                                myProposeSequenceNumber);
                    }

                    //create a new message and send it back
                    Message proposeMessage = new Message();
                    proposeMessage.setSrc(localAddress);
                    proposeMessage.setDest(messageID.getAddress());

                    GroupMulticastHeader newHeader = GroupMulticastHeader.createNewHeader(
                            GroupMulticastHeader.SEQ_NO_PROPOSE, messageID);

                    newHeader.setSequencerNumber(myProposeSequenceNumber);
                    proposeMessage.putHeader(this.id, newHeader);
                    proposeMessage.setFlag(Message.Flag.OOB);
                    proposeMessage.setFlag(Message.Flag.DONT_BUNDLE);

                    //multicastSenderThread.addUnicastMessage(proposeMessage);
                    down_prot.down(new Event(Event.MSG, proposeMessage));
                    break;
                case GroupMulticastHeader.SEQ_NO_PROPOSE:
                    if (log.isTraceEnabled()) {
                        log.trace("Received the proposed sequence number message with " + header + " from " +
                                message.getSrc());
                    }

                    sequenceNumberManager.update(header.getSequencerNumber());
                    long finalSequenceNumber = senderManager.addPropose(messageID, message.getSrc(),
                            header.getSequencerNumber());

                    if (finalSequenceNumber != SenderManager.NOT_READY) {
                        Message finalMessage = new Message();
                        finalMessage.setSrc(localAddress);

                        GroupMulticastHeader finalHeader = GroupMulticastHeader.createNewHeader(
                                GroupMulticastHeader.SEQ_NO_FINAL, messageID);

                        finalHeader.setSequencerNumber(finalSequenceNumber);
                        finalMessage.putHeader(this.id, finalHeader);
                        finalMessage.setFlag(Message.Flag.OOB);
                        finalMessage.setFlag(Message.Flag.DONT_BUNDLE);

                        Set<Address> destination = senderManager.getDestination(messageID);
                        if (destination.contains(localAddress)) {
                            destination.remove(localAddress);
                        }

                        if (log.isTraceEnabled()) {
                            log.trace("Message " + messageID + " is ready to be deliver. Final sequencer number is " +
                                    finalSequenceNumber);
                        }
                        
                        multicastSenderThread.addMessage(finalMessage, destination);
                        //returns true if we are in destination set
                        if (senderManager.markSent(messageID)) {
                            deliverManager.markReadyToDeliver(messageID, finalSequenceNumber);
                        }
                    }

                    break;
                case GroupMulticastHeader.SEQ_NO_FINAL:
                    if (log.isTraceEnabled()) {
                        log.trace("Received the final sequence number message with " + header);
                    }

                    sequenceNumberManager.update(header.getSequencerNumber());
                    deliverManager.markReadyToDeliver(messageID, header.getSequencerNumber());
                    break;
                default:
                    throw new IllegalStateException("Unknown header type received " + header);
            }
        } catch (/*Interrupted*/Exception e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
    }

    @Override
    public void deliver(Message message) {
        message.setDest(localAddress);

        if (log.isDebugEnabled()) {
            log.debug("Deliver message " + message + " in total order");
        }

        up_prot.up(new Event(Event.MSG, message));
    }

    @ManagedOperation
    public String getMessageList() {
        return deliverManager.getMessageSet().toString();
    }
}

package org.jgroups.protocols.pmcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.groups.GroupAddress;
import org.jgroups.groups.MessageID;
import org.jgroups.groups.header.GroupMulticastHeader2;
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
 * @author pruivo
 * @since 4.0
 */
public class GROUP_MULTICAST5 extends Protocol {
    //managers
    private DeliverManagerImpl deliverManager;
    private SenderManager senderManager;

    //thread
    private final DeliverThread deliverThread;
    private final SenderThread senderThread;

    //local address
    private Address localAddress;

    //sequence numbers, messages ids and lock
    private final SequenceNumberManager sequenceNumberManager;
    private long messageIdCounter;
    private final Lock sendLock;

    public GROUP_MULTICAST5() {
        deliverThread = new DeliverThread(this);
        senderThread = new SenderThread(this);
        sequenceNumberManager = new SequenceNumberManager();
        sendLock = new ReentrantLock();
        messageIdCounter = 0;
    }

    @Override
    public void start() throws Exception {
        deliverManager = new DeliverManagerImpl();
        senderManager = new SenderManager();
        deliverThread.start(deliverManager);
        senderThread.clear();
        senderThread.start();
    }

    @Override
    public void stop() {
        deliverThread.interrupt();
        senderThread.interrupt();
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
            default:
                break;
        }
        return up_prot.up(evt);
    }

    private void handleViewChange(View view) {
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
        GroupAddress groupAddress = (GroupAddress) message.getDest();
        try {
            senderThread.addMessage(message, groupAddress.getAddresses());
        } catch (InterruptedException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
    }

    private void handleDownGroupMulticastMessage(Message message) {
        GroupAddress groupAddress = (GroupAddress) message.getDest();
        Set<Address> destination = groupAddress.getAddresses();

        if (destination.isEmpty()) {
            throw new IllegalStateException("Group Address must have at least two elements");
        }

        if (destination.size() == 1) {
            message.setDest(destination.iterator().next());
            down_prot.down(new Event(Event.MSG, message));
            return;
        }

        boolean localAddressInDestination = destination.contains(localAddress);

        try {
            sendLock.lock();
            MessageID messageID = new MessageID(localAddress, messageIdCounter++);
            long sequenceNumber = sequenceNumberManager.getAndIncrement();

            GroupMulticastHeader2 header2 = GroupMulticastHeader2.createNewHeader(GroupMulticastHeader2.MESSAGE,
                    messageID);
            header2.setSequencerNumber(sequenceNumber);
            header2.addDestinations(destination);
            message.putHeader(this.id, header2);

            senderManager.addNewMessageToSent(messageID, destination, sequenceNumber);

            if (localAddressInDestination) {
                Set<Address> destinationWithoutLocalAddress = new HashSet<Address>(destination);
                destinationWithoutLocalAddress.remove(localAddress);

                deliverManager.addNewMessageToDeliver(messageID, message.getObject(), sequenceNumber);

                senderThread.addMessage(message, destinationWithoutLocalAddress);
            } else {
                senderThread.addMessage(message, destination);
            }


        } catch (InterruptedException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        } finally {
            sendLock.unlock();
        }
    }

    private void handleUpMessage(Event evt) {
        Message message = (Message) evt.getArg();

        GroupMulticastHeader2 header2 = (GroupMulticastHeader2) message.getHeader(this.id);

        if (header2 == null) {
            up_prot.up(evt);
            return;
        }

        MessageID messageID = header2.getMessageID();
        try {
            switch (header2.getType()) {
                case GroupMulticastHeader2.MESSAGE:
                    //create the sequence number and put it in deliver manager
                    long myProposeSequenceNumber = sequenceNumberManager.updateAndGet(header2.getSequencerNumber());
                    deliverManager.addNewMessageToDeliver(messageID, message.getObject(), myProposeSequenceNumber);

                    //create a new message and send it back
                    Message proposeMessage = new Message();
                    proposeMessage.setSrc(localAddress);
                    proposeMessage.setDest(messageID.getAddress());

                    GroupMulticastHeader2 newHeader = GroupMulticastHeader2.createNewHeader(
                            GroupMulticastHeader2.SEQ_NO_PROPOSE, messageID);

                    newHeader.setSequencerNumber(myProposeSequenceNumber);
                    proposeMessage.putHeader(this.id, newHeader);

                    senderThread.addUnicastMessage(proposeMessage);
                    break;
                case GroupMulticastHeader2.SEQ_NO_PROPOSE:
                    long finalSequenceNumber = senderManager.addPropose(messageID, message.getSrc(),
                            header2.getSequencerNumber());

                    if (finalSequenceNumber != SenderManager.NOT_READY) {
                        Message finalMessage = new Message();
                        finalMessage.setSrc(localAddress);

                        GroupMulticastHeader2 finalHeader = GroupMulticastHeader2.createNewHeader(
                                GroupMulticastHeader2.SEQ_NO_FINAL, messageID);

                        finalHeader.setSequencerNumber(finalSequenceNumber);
                        finalMessage.putHeader(this.id, finalHeader);

                        Set<Address> destination = senderManager.getDestination(messageID);
                        if (destination.contains(localAddress)) {
                            destination.remove(localAddress);
                        }

                        senderThread.addMessage(finalMessage, destination);
                        senderManager.markSent(messageID);
                        deliverManager.markReadyToDeliver(messageID, finalSequenceNumber);
                    }

                    break;
                case GroupMulticastHeader2.SEQ_NO_FINAL:
                    deliverManager.markReadyToDeliver(messageID, header2.getSequencerNumber());
                    break;
                default:
                    throw new IllegalStateException("Unknown header type received " + header2);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
    }
}

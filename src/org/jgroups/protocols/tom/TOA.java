package org.jgroups.protocols.tom;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Total Order Anycast with three communication steps (based on Skeen's Algorithm). Establishes total order for a
 * message sent to a subset of the cluster members (an anycast). Example: send a totally ordered message to {D,E}
 * out of a membership of {A,B,C,D,E,F}.<p/>
 * Skeen's algorithm uses consensus among the anycast target members to find the currently highest
 * sequence number (seqno) and delivers the message according to the order established by the seqnos.
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
@MBean(description = "Implementation of Total Order Anycast based on Skeen's Algorithm")
public class TOA extends Protocol {
    //managers
    private DeliverManager<MessageID, Message> deliverManager;
    private SendManager<MessageID, Address> senderManager;

    // threads
    private final DeliveryThread deliverThread = new DeliveryThread(this);

    //local address
    private Address localAddress;

    //sequence numbers, messages ids and lock
    private final AtomicLong messageIdCounter = new AtomicLong(0);

    //stats: profiling information
    private final StatsCollector statsCollector = new StatsCollector();
    private volatile View currentView;

    private final boolean trace = log.isTraceEnabled();
    private final boolean debug = log.isDebugEnabled();

    public TOA() {
    }

    @Override
    public void start() throws Exception {
        deliverManager = new DeliverManager<>();
        senderManager = new SendManager<>();
        deliverThread.start(deliverManager);
        statsCollector.setStatsEnabled(statsEnabled());
    }

    @Override
    public void stop() {
        deliverThread.interrupt();
    }

    @Override
    public Object down(Event evt) {
        switch (evt.getType()) {
            case Event.MSG:
                Message message = (Message) evt.getArg();
                Address dest = message.getDest();

                if (dest != null && dest instanceof AnycastAddress && !message.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
                    //anycast message
                    sendTotalOrderAnycastMessage(extract((AnycastAddress) dest), message);
                } else if (dest != null && dest instanceof AnycastAddress) {
                    //anycast address with NO_TOTAL_ORDER flag (should no be possible, but...)
                    send(extract((AnycastAddress) dest), message, false);
                } else {
                    //normal message
                    break;
                }
                return null;
            case Event.SET_LOCAL_ADDRESS:
                setLocalAddress((Address) evt.getArg());
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
                Message message = (Message) evt.getArg();

                ToaHeader header = (ToaHeader) message.getHeader(this.id);

                if (header == null) {
                    break;
                }

                switch (header.getType()) {
                    case ToaHeader.DATA_MESSAGE:
                        handleDataMessage(message, header);
                        break;
                    case ToaHeader.PROPOSE_MESSAGE:
                        handleSequenceNumberPropose(message.getSrc(), header);
                        break;
                    case ToaHeader.FINAL_MESSAGE:
                        handleFinalSequenceNumber(header);
                        break;
                    case ToaHeader.SINGLE_DESTINATION_MESSAGE:
                        if (trace) {
                            log.trace(String.format("Received message '%s' with SINGLE_DESTINATION header.", header.getMessageID()));
                        }
                        deliverManager.addReadyToDeliver(header.getMessageID(), message);
                        break;
                    default:
                        throw new IllegalStateException("Unknown header type received " + header);
                }
                return null;
            case Event.VIEW_CHANGE:
                handleViewChange((View) evt.getArg());
                break;
            case Event.SET_LOCAL_ADDRESS:
                setLocalAddress((Address) evt.getArg());
                break;
            default:
                break;
        }
        return up_prot.up(evt);
    }

    public void deliver(Message message) {
        message.setDest(localAddress);

        if (debug) {
            log.debug(String.format("Deliver message '%s' in total order", message));
        }

        up_prot.up(new Event(Event.MSG, message));
        statsCollector.incrementMessageDeliver();
    }

    private void setLocalAddress(Address localAddress) {
        this.localAddress = localAddress;
        this.deliverThread.setLocalAddress(localAddress.toString());
    }

    private void handleViewChange(View view) {
        if (trace) {
            log.trace(String.format("Handling a new view: %s", view));
        }
        //View oldView = currentView;
        currentView = view;
        /*
        //basis behavior: drop leavers message (as senders)
        List<Address> leavers = View.leftMembers(oldView, view);
        deliverManager.removeLeavers(leavers);

        //basis behavior: avoid waiting for the acks
        Collection<MessageID> pendingSentMessages = senderManager.getPendingMessageIDs();
        for (MessageID messageID : pendingSentMessages) {
            long finalSequenceNumber = senderManager.removeLeavers(messageID, leavers);
            if (finalSequenceNumber != SenderManager.NOT_READY) {
                ToaHeader finalHeader = ToaHeader.newFinalMessageHeader(messageID, finalSequenceNumber);
                Message finalMessage = new Message().src(localAddress).putHeader(this.id, finalHeader)
                        .setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE);

                Set<Address> destinations = senderManager.getDestination(messageID);
                if (destinations.contains(localAddress)) {
                    destinations.remove(localAddress);
                }

                if (log.isTraceEnabled()) {
                    log.trace("Message " + messageID + " is ready to be deliver. Final sequencer number is " +
                            finalSequenceNumber);
                }

                send(destinations, finalMessage, false);
                //returns true if we are in destination set
                if (senderManager.markSent(messageID)) {
                    deliverManager.markReadyToDeliver(messageID, finalSequenceNumber);
                }
            }
        }
        */
        // TODO: Future work: How to add fault tolerance? (simple and efficient)
    }

    private void sendTotalOrderAnycastMessage(List<Address> destinations, Message message) {
        long startTime = statsCollector.now();
        long duration = -1;

        final MessageID messageID = generateId();
        final boolean deliverToMySelf = destinations.contains(localAddress);

        if (destinations.size() == 1) {
            message.putHeader(id, ToaHeader.createSingleDestinationHeader(messageID));
            message.setDest(destinations.get(0));

            if (trace) {
                log.trace(String.format("Sending total order anycast message (id='%s') to a single destination '%s'",
                        messageID, message.getDest()));
            }

            if (deliverToMySelf) {
                deliverManager.addReadyToDeliver(messageID, message);
            } else {
                down_prot.down(new Event(Event.MSG, message));
            }
            return;
        }

        try {
            long sequenceNumber = -1;
            if (deliverToMySelf) {
                sequenceNumber = deliverManager.put(messageID, message, sequenceNumber);
            }
            message.putHeader(this.id, ToaHeader.newDataMessageHeader(messageID, sequenceNumber));

            if (trace) {
                log.trace(String.format("Sending total order anycast message (id='%s') to a multiple destination '%s'",
                        messageID, destinations));
            }

            senderManager.put(messageID, destinations, localAddress, sequenceNumber);
            send(destinations, message, true);
            duration = statsCollector.now() - startTime;
        } catch (Exception e) {
            logException("Exception caught while sending anycast message. Error is " + e.getLocalizedMessage(), e);
        } finally {
            statsCollector.addAnycastSentDuration(duration, (destinations.size() - (deliverToMySelf ? 1 : 0)));
        }
    }

    private MessageID generateId() {
        return new MessageID(localAddress, messageIdCounter.getAndIncrement());
    }

    private void send(Collection<Address> destinations, Message msg, boolean excludeLocal) {
        for (Address address : destinations) {
            if (excludeLocal && address.equals(localAddress)) {
                continue;
            }
            Message cpy = msg.copy();
            cpy.setDest(address);
            down_prot.down(new Event(Event.MSG, cpy));
        }
    }

    private void handleDataMessage(Message message, ToaHeader header) {
        long startTime = statsCollector.now();
        long duration = -1;

        try {
            final MessageID messageID = header.getMessageID();

            //create the sequence number and put it in deliver manager
            long myProposeSequenceNumber = deliverManager.put(messageID, message, header.getSequencerNumber());

            if (trace) {
                log.trace(String.format("Received data message (id='%s'). My SeqNo=%s (received SeqNo=%s).",
                        messageID, myProposeSequenceNumber, header.getSequencerNumber()));
            }

            //create a new message and send it back
            Message proposeMessage = new Message().src(localAddress).dest(messageID.getAddress())
                    .putHeader(this.id, ToaHeader.newProposeMessageHeader(messageID, myProposeSequenceNumber))
                    .setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE);

            down_prot.down(new Event(Event.MSG, proposeMessage));
            duration = statsCollector.now() - startTime;
        } catch (Exception e) {
            logException("Exception caught while processing the data message " + header.getMessageID(), e);
        } finally {
            statsCollector.addDataMessageDuration(duration);
        }
    }

    private void handleSequenceNumberPropose(Address from, ToaHeader header) {
        long startTime = statsCollector.now();
        long duration = -1;
        boolean lastProposeReceived = false;

        try {
            MessageID messageID = header.getMessageID();
            if (trace) {
                log.trace(String.format("Received propose sequence number from '%s' for message (id='%s'). SeqNo=%s",
                        from, header.getMessageID(), header.getSequencerNumber()));
            }

            deliverManager.updateSequenceNumber(header.getSequencerNumber());
            long finalSequenceNumber = senderManager.addPropose(messageID, from, header.getSequencerNumber());

            if (finalSequenceNumber != SendManager.NOT_READY) {
                lastProposeReceived = true;

                Message finalMessage = new Message().src(localAddress)
                        .putHeader(this.id, ToaHeader.newFinalMessageHeader(messageID, finalSequenceNumber))
                        .setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE);

                List<Address> destinations = senderManager.getElements(messageID);

                if (trace) {
                    log.trace(String.format("Message (id='%s') is ready to deliver.SeqNo=%s", messageID, finalSequenceNumber));
                }

                send(destinations, finalMessage, true);
                deliverManager.markFinal(messageID, finalSequenceNumber);
                senderManager.addDelivered(messageID, localAddress);
            }

            duration = statsCollector.now() - startTime;
        } catch (Exception e) {
            logException("Exception caught while processing the propose sequence number for " + header.getMessageID(), e);
        } finally {
            statsCollector.addProposeSequenceNumberDuration(duration, lastProposeReceived);
        }
    }

    private void handleFinalSequenceNumber(ToaHeader header) {
        long startTime = statsCollector.now();
        long duration = -1;

        try {
            MessageID messageID = header.getMessageID();
            if (trace) {
                log.trace(String.format("Received final sequence number for message (id='%s'). SeqNo=%s",
                        messageID, header.getSequencerNumber()));
            }

            deliverManager.markFinal(messageID, header.getSequencerNumber());
            duration = statsCollector.now() - startTime;
        } catch (Exception e) {
            logException("Exception caught while processing the final sequence number for " + header.getMessageID(), e);
        } finally {
            statsCollector.addFinalSequenceNumberDuration(duration);
        }
    }

    private void logException(String msg, Exception e) {
        if (debug) {
            log.debug(msg, e);
        } else if (log.isWarnEnabled()) {
            log.warn(msg + ". Error is " + e.getLocalizedMessage());
        }
    }

    private List<Address> extract(AnycastAddress anycastAddress) {
        Collection<Address> addresses = anycastAddress.getAddresses();
        if (addresses == null) {
            return new ArrayList<>(currentView.getMembers());
        } else {
            return new ArrayList<>(addresses);
        }
    }

    @ManagedOperation
    public String getMessageList() {
        //return deliverManager.getMessageSet().toString();
        return null;
    }

    @Override
    public void enableStats(boolean flag) {
        super.enableStats(flag);
        statsCollector.setStatsEnabled(flag);
    }

    @Override
    public void resetStats() {
        super.resetStats();
        statsCollector.clearStats();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing and sending the anycast " +
            "message to all the recipients", writable = false)
    public double getAvgToaSendDuration() {
        return statsCollector.getAvgAnycastSentDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing a data message received",
            writable = false)
    public double getAvgDataMessageReceivedDuration() {
        return statsCollector.getAvgDataMessageReceivedDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing a propose message received" +
            "(not the last one", writable = false)
    public double getAvgProposeMessageReceivedDuration() {
        return statsCollector.getAvgProposeMesageReceivedDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing the last propose message " +
            "received. This last propose message will originate the sending of the final message", writable = false)
    public double getAvgLastProposeMessageReceivedDuration() {
        return statsCollector.getAvgLastProposeMessageReceivedDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing a final message received",
            writable = false)
    public double getAvgFinalMessageReceivedDuration() {
        return statsCollector.getAvgFinalMessageReceivedDuration();
    }

    @ManagedAttribute(description = "The number of anycast messages sent", writable = false)
    public int getNumberOfAnycastMessagesSent() {
        return statsCollector.getNumberOfAnycastMessagesSent();
    }

    @ManagedAttribute(description = "The number of final anycast sent", writable = false)
    public int getNumberOfFinalAnycastSent() {
        return statsCollector.getNumberOfFinalAnycastsSent();
    }

    @ManagedAttribute(description = "The number of anycast messages delivered", writable = false)
    public int getNumberOfAnycastMessagesDelivered() {
        return statsCollector.getAnycastDelivered();
    }

    @ManagedAttribute(description = "The number of propose messages sent", writable = false)
    public int getNumberOfProposeMessageSent() {
        return statsCollector.getNumberOfProposeMessagesSent();
    }

    @ManagedAttribute(description = "The number of final messages delivered", writable = false)
    public int getNumberOfFinalMessagesDelivered() {
        return statsCollector.getNumberOfFinalMessagesDelivered();
    }

    @ManagedAttribute(description = "The number of data messages delivered", writable = false)
    public int getNumberOfDataMessagesDelivered() {
        return statsCollector.getNumberOfProposeMessagesSent();
    }

    @ManagedAttribute(description = "The number of propose messages received", writable = false)
    public int getNumberOfProposeMessageReceived() {
        return statsCollector.getNumberOfProposeMessagesReceived();
    }

    @ManagedAttribute(description = "The average number of unicasts messages created per anycast message",
            writable = false)
    public double getAvgNumberOfUnicastSentPerAnycast() {
        return statsCollector.getAvgNumberOfUnicastSentPerAnycast();
    }
}

package org.jgroups.groups.manager;

import org.jgroups.Address;
import org.jgroups.groups.MessageID;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class SenderManager {

    public static final long NOT_READY = -1;

    private final ConcurrentMap<MessageID, MessageInfo> sentMessages = new ConcurrentHashMap<MessageID, MessageInfo>();

    public void addNewMessageToSent(MessageID messageID, Set<Address> destination, long initialSequenceNumber) {
        MessageInfo messageInfo = new MessageInfo(destination, initialSequenceNumber);
        messageInfo.setProposeReceived(messageID.getAddress());
        sentMessages.put(messageID, messageInfo);
    }

    public long addPropose(MessageID messageID, Address from, long sequenceNumber) {
        MessageInfo messageInfo = sentMessages.get(messageID);
        if (messageInfo != null && messageInfo.addPropose(from, sequenceNumber)) {
            return messageInfo.getAndMarkFinalSent();
        }
        return NOT_READY;
    }

    public void markSent(MessageID messageID) {
        sentMessages.remove(messageID);
    }

    public Set<Address> getDestination(MessageID messageID) {
        MessageInfo messageInfo = sentMessages.get(messageID);
        Set<Address> destination;
        if (messageInfo != null) {
            destination = new HashSet<Address>(messageInfo.destination);
        } else {
            destination = Collections.emptySet();
        }
        return destination;
    }

    public void clear() {
        sentMessages.clear();
    }

    private static class MessageInfo {
        private ArrayList<Address> destination;
        private long highestSequenceNumberReceived;
        private BitSet receivedPropose;
        private boolean finalMessageSent = false;

        private MessageInfo(Set<Address> addresses, long sequenceNumber) {
            this.destination = new ArrayList<Address>(addresses);
            this.highestSequenceNumberReceived = sequenceNumber;
            createNewBitSet(addresses.size());
        }

        private synchronized boolean addPropose(Address from, long sequenceNumber) {
            setProposeReceived(from);
            highestSequenceNumberReceived = Math.max(highestSequenceNumberReceived, sequenceNumber);
            return checkAllProposesReceived();
        }

        private synchronized long getAndMarkFinalSent() {
            if (checkAllProposesReceived() && !finalMessageSent) {
                finalMessageSent = true;
                return highestSequenceNumberReceived;
            }
            return NOT_READY;
        }

        private void createNewBitSet(int maxElements) {
            receivedPropose = new BitSet(maxElements);
            for (int i = 0; i < maxElements; ++i) {
                receivedPropose.set(i);
            }
        }

        private void setProposeReceived(Address address) {
            int idx = destination.indexOf(address);
            if (idx == -1) {
                throw new IllegalStateException("Address doesn't exists in destination list. Address is " + address);
            }
            receivedPropose.set(idx, false);
        }

        private boolean checkAllProposesReceived() {
            return receivedPropose.isEmpty();
        }
    }

}

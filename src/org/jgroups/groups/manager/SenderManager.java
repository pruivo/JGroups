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
        private static final int BYTE_LENGTH = 7; //java doesn't have unsigned byte so we just ignore the last bit

        private ArrayList<Address> destination;
        private long highestSequenceNumberReceived;
        private byte[] receivedPropose;
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
            receivedPropose = new byte[(maxElements - 1) / BYTE_LENGTH];
            for (int i = 0; i < receivedPropose.length - 1; ++i) {
                receivedPropose[i] = 0x7f;
            }
            for (int i = 0; i < (maxElements - 1) % BYTE_LENGTH; ++i) {
                receivedPropose[receivedPropose.length - 1] |= 1 << i;
            }
        }

        private void setProposeReceived(Address address) {
            int idx = destination.indexOf(address);
            if (idx == -1) {
                throw new IllegalStateException("Address doesn't exists in destination list. Address is " + address);
            }
            byte mask = (byte) ~(1 << (idx % BYTE_LENGTH));
            receivedPropose[idx / BYTE_LENGTH] &= mask;
        }

        private boolean checkAllProposesReceived() {
            for (int i = 0; i < receivedPropose.length; ++i) {
                if (receivedPropose[i] != 0) {
                    return false;
                }
            }
            return true;
        }
    }

}

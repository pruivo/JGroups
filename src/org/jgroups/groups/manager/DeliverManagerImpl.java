package org.jgroups.groups.manager;

import org.jgroups.Message;
import org.jgroups.groups.MessageID;

import java.util.*;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class DeliverManagerImpl implements DeliverManager {
    private static final MessageInfoComparator COMPARATOR = new MessageInfoComparator();
    private final SortedSet<MessageInfo> toDeliverSet = new TreeSet<MessageInfo>(COMPARATOR);

    public void addNewMessageToDeliver(MessageID messageID, Object message, long sequenceNumber) {
        MessageInfo messageInfo = new MessageInfo(messageID, message, sequenceNumber);
        synchronized (toDeliverSet) {
            toDeliverSet.add(messageInfo);
        }
    }

    @SuppressWarnings({"SuspiciousMethodCalls"})
    public void markReadyToDeliver(MessageID messageID, long finalSequenceNumber) {
        synchronized (toDeliverSet) {
            MessageInfo messageInfo = null;
            boolean needsUpdatePosition = false;
            Iterator<MessageInfo> iterator = toDeliverSet.iterator();

            while (iterator.hasNext()) {
                MessageInfo aux = iterator.next();
                if (aux.equals(messageID)) {
                    messageInfo = aux;
                    if (messageInfo.sequenceNumber != finalSequenceNumber) {
                        needsUpdatePosition = true;
                        iterator.remove();
                    }
                    break;
                }
            }

            if (messageInfo == null) {
                throw new IllegalStateException("Message ID not found in to deliver list. this can't happen. " +
                        "Message ID is " + messageID);
            }
            messageInfo.updateAndmarkReadyToDeliver(finalSequenceNumber);
            if (needsUpdatePosition) {
                toDeliverSet.add(messageInfo);
            }

            if (!toDeliverSet.isEmpty() && toDeliverSet.first().isReadyToDeliver()) {
                toDeliverSet.notify();
            }
        }
    }

    public List<Message> getNextMessagesToDeliver() throws InterruptedException {
        LinkedList<Message> toDeliver = new LinkedList<Message>();
        synchronized (toDeliverSet) {
            while (toDeliverSet.isEmpty()) {
                toDeliverSet.wait();
            }

            if (!toDeliverSet.first().isReadyToDeliver()) {
                toDeliverSet.wait();
            }

            Iterator<MessageInfo> iterator = toDeliverSet.iterator();

            while (iterator.hasNext()) {
                MessageInfo messageInfo = iterator.next();
                if (messageInfo.isReadyToDeliver()) {
                    toDeliver.add(messageInfo.createMessage());
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
        return toDeliver;
    }

    public void clear() {
        synchronized (toDeliverSet) {
            toDeliverSet.clear();
        }
    }

    private static class MessageInfo {

        private MessageID messageID;
        private Object message;
        private volatile long sequenceNumber;
        private volatile boolean readyToDeliver;

        public MessageInfo(MessageID messageID, Object message, long sequenceNumber) {
            if (messageID == null) {
                throw new NullPointerException("Message ID can't be null");
            }
            this.messageID = messageID;
            this.message = message;
            this.sequenceNumber = sequenceNumber;
            this.readyToDeliver = false;
        }

        private Message createMessage() {
            Message message = new Message();
            message.setSrc(messageID.getAddress());
            message.setObject(this.message);
            return message;
        }

        private void updateAndmarkReadyToDeliver(long finalSequenceNumber) {
            this.readyToDeliver = true;
            this.sequenceNumber = finalSequenceNumber;
        }

        private boolean isReadyToDeliver() {
            return readyToDeliver;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null) {
                return false;
            }

            boolean isMessageID = o.getClass() == MessageID.class;

            if (o.getClass() != getClass() && !isMessageID) {
                return false;
            }

            if (isMessageID) {
                return messageID.equals(o);
            }

            MessageInfo that = (MessageInfo) o;

            return messageID.equals(that.messageID);
        }

        @Override
        public int hashCode() {
            return messageID.hashCode();
        }

        @Override
        public String toString() {
            return "MessageInfo{" +
                    "messageID=" + messageID +
                    ", sequenceNumber=" + sequenceNumber +
                    ", readyToDeliver=" + readyToDeliver +
                    '}';
        }
    }

    private static class MessageInfoComparator implements Comparator<MessageInfo> {

        @Override
        public int compare(MessageInfo messageInfo, MessageInfo messageInfo1) {
            if (messageInfo == null) {
                return messageInfo1 == null ? 0 : 1;
            } else if (messageInfo1 == null) {
                return -1;
            }

            int compareMessageID = messageInfo.messageID.compareTo(messageInfo1.messageID);

            if (compareMessageID == 0) {
                return 0;
            }

            if (messageInfo.sequenceNumber != messageInfo1.sequenceNumber) {
                return Long.signum(messageInfo.sequenceNumber - messageInfo1.sequenceNumber);
            }

            return compareMessageID;
        }
    }

    public Set<MessageInfo> getMessageSet() {
        synchronized (toDeliverSet) {
            return Collections.unmodifiableSet(toDeliverSet);
        }
    }
}

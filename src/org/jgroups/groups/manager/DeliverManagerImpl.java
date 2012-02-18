package org.jgroups.groups.manager;

import org.jgroups.Message;
import org.jgroups.groups.MessageID;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class DeliverManagerImpl implements DeliverManager {
    private static final MessageInfoComparator COMPARATOR = new MessageInfoComparator();
    private final LinkedList<MessageInfo> toDeliverList = new LinkedList<MessageInfo>();

    public void addNewMessageToDeliver(MessageID messageID, Object message, long sequenceNumber) {
        MessageInfo messageInfo = new MessageInfo(messageID, message, sequenceNumber);
        synchronized (toDeliverList) {
            ListIterator<MessageInfo> iterator = toDeliverList.listIterator(toDeliverList.size());

            int insertIdx = 0;

            while (iterator.hasPrevious()) {
                if (COMPARATOR.compare(iterator.previous(), messageInfo) < 0) {
                    insertIdx = iterator.nextIndex() - 1;
                    break;
                }
            }

            toDeliverList.add(insertIdx, messageInfo);
        }
    }

    @SuppressWarnings({"SuspiciousMethodCalls"})
    public void markReadyToDeliver(MessageID messageID, long finalSequenceNumber) {
        synchronized (toDeliverList) {
            int idx = toDeliverList.indexOf(messageID);

            if (idx == -1) {
                throw new IllegalStateException("Message ID not found in to deliver list. this can't happen. " +
                        "Message ID is " + messageID);
            }

            MessageInfo messageInfo = toDeliverList.get(idx);

            if (messageInfo.updateAndmarkReadyToDeliver(finalSequenceNumber)) {
                toDeliverList.remove(idx);

                for (; idx < toDeliverList.size(); ++idx) {
                    if (COMPARATOR.compare(toDeliverList.get(idx), messageInfo) < 0) {
                        toDeliverList.add(idx, messageInfo);
                    }
                }
            }

            if (idx == 0) {
                toDeliverList.notify();
            }
        }
    }

    public List<Message> getNextMessagesToDeliver() throws InterruptedException {
        LinkedList<Message> toDeliver = new LinkedList<Message>();
        synchronized (toDeliverList) {
            while (toDeliverList.isEmpty()) {
                toDeliverList.wait();
            }

            if (!toDeliverList.get(0).isReadyToDeliver()) {
                toDeliverList.wait();
            }

            ListIterator<MessageInfo> iterator = toDeliverList.listIterator();

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
        synchronized (toDeliverList) {
            toDeliverList.clear();
        }
    }

    private static class MessageInfo {

        private MessageID messageID;
        private Object message;
        private long sequenceNumber;
        private boolean readyToDeliver;

        public MessageInfo(MessageID messageID, Object message, long sequenceNumber) {
            if (messageID == null) {
                throw new NullPointerException("Message ID can't be null");
            }
            this.messageID = messageID;
            this.message = message;
            this.sequenceNumber = sequenceNumber;
            readyToDeliver = false;
        }

        private Message createMessage() {
            Message message = new Message();
            message.setSrc(messageID.getAddress());
            message.setObject(this.message);
            return message;
        }

        private boolean updateAndmarkReadyToDeliver(long finalSequenceNumber) {
            boolean result = this.sequenceNumber != finalSequenceNumber;
            this.readyToDeliver = true;
            this.sequenceNumber = finalSequenceNumber;
            return result;
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
}

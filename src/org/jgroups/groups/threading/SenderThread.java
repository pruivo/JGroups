package org.jgroups.groups.threading;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class SenderThread extends Thread {
    private boolean running = false;
    private Protocol groupMulticastProtocol;
    private final BlockingQueue<MessageToSend> sendingQueue;

    public SenderThread(Protocol protocol) {
        if (protocol == null) {
            throw new NullPointerException("Group Multicast Protocol can't be null");
        }
        this.groupMulticastProtocol = protocol;
        this.sendingQueue = new LinkedBlockingQueue<MessageToSend>();
    }

    public void addMessage(Message message, Set<Address> destination) throws InterruptedException {
        sendingQueue.put(new MessageToSend(message, destination));
    }

    public void clear() {
        sendingQueue.clear();
    }

    public void addUnicastMessage(Message message) throws InterruptedException {
        sendingQueue.put(new MessageToSend(message, null));
    }

    @Override
    public void start() {
        running = true;
        super.start();
    }

    @Override
    public void run() {
        while (running) {
            try {
                MessageToSend messageToSend = sendingQueue.take();

                if (messageToSend.destination == null) {
                    groupMulticastProtocol.getDownProtocol().down(new Event(Event.MSG, messageToSend.message));
                } else {
                    for (Address address : messageToSend.destination) {
                        Message cpy = messageToSend.message.copy();
                        cpy.setDest(address);
                        groupMulticastProtocol.getDownProtocol().down(new Event(Event.MSG, cpy));
                    }
                }
            } catch (InterruptedException e) {
                //interrupted
            }
        }
    }

    @Override
    public void interrupt() {
        running = false;
        super.interrupt();
    }

    private class MessageToSend {
        private Message message;
        private Set<Address> destination;

        private MessageToSend(Message message, Set<Address> destination) {
            this.message = message;
            this.destination = destination;
        }
    }
}

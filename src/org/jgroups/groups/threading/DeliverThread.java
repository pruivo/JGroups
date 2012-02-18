package org.jgroups.groups.threading;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.groups.manager.DeliverManager;
import org.jgroups.stack.Protocol;

import java.util.List;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class DeliverThread extends Thread {
    private DeliverManager deliverManager;
    private boolean running = false;
    private Protocol groupMulticastProtocol;

    public DeliverThread(Protocol protocol) {
        if (protocol == null) {
            throw new NullPointerException("Group Multicast Protocol can't be null");
        }
        this.groupMulticastProtocol = protocol;
    }

    public void start(DeliverManager deliverManager) {
        this.deliverManager = deliverManager;
        start();
    }

    @Override
    public void start() {
        if (deliverManager == null) {
            throw new NullPointerException("Deliver Manager can't be null");
        }
        running = true;
        super.start();
    }

    @Override
    public void run() {
        while (running) {
            try {
                List<Message> messages = deliverManager.getNextMessagesToDeliver();

                for (Message msg : messages) {
                    groupMulticastProtocol.getUpProtocol().up(new Event(Event.MSG, msg));
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
}

package org.jgroups.protocols.tom;

import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

/**
 * The delivery thread. Is the only thread that delivers the Total Order Anycast message in order
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class DeliveryThread extends Thread {
    private final Log log = LogFactory.getLog(this.getClass());
    private DeliverManager<?, Message> deliveryManager;
    private volatile boolean running = false;
    private TOA deliveryProtocol;

    public DeliveryThread(TOA protocol) {
        super("TOA-Delivery-Thread");
        if (protocol == null) {
            throw new NullPointerException("TOA Protocol can't be null");
        }
        this.deliveryProtocol = protocol;
    }

    public void start(DeliverManager<?, Message> deliveryManager) {
        this.deliveryManager = deliveryManager;
        start();
    }

    public void setLocalAddress(String localAddress) {
        setName("TOA-Delivery-Thread-" + localAddress);
    }

    @Override
    public void start() {
        if (deliveryManager == null) {
            throw new NullPointerException("Delivery Manager can't be null");
        }
        running = true;
        super.start();
    }

    @Override
    public void run() {
        Message[] buffer = new Message[32];
        while (running) {
            try {
                final int ready = deliveryManager.deliverReady(buffer);
                for (int i = 0; i < ready; ++i) {
                    try {
                        deliveryProtocol.deliver(buffer[i]);
                    } catch (Throwable t) {
                        log.warn("Exception caught while delivering message " + buffer[i] + ":" + t.getMessage());
                    } finally {
                        buffer[i] = null;
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
}

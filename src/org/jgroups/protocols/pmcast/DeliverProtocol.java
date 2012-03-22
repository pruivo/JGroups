package org.jgroups.protocols.pmcast;

import org.jgroups.Message;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public interface DeliverProtocol {
    void deliver(Message message);
}

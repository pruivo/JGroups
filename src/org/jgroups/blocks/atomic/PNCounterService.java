package org.jgroups.blocks.atomic;

import org.jgroups.JChannel;
import org.jgroups.protocols.PN_COUNTER;

import java.util.Objects;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class PNCounterService {

    private volatile PN_COUNTER protocol;

    public PNCounterService(JChannel channel) {
        setChannel(channel);
    }

    public void setChannel(JChannel channel) {
        PN_COUNTER p = Objects.requireNonNull(channel, "JChannel must be non-null!")
                .getProtocolStack()
                .findProtocol(PN_COUNTER.class);
        if (p == null)
            throw new IllegalStateException("JChannel configuration must include the PN_COUNTER protocol");
        protocol = p;
    }

    PNCounter getOrCreateCounter(String name) {
        return protocol.getOrCreateCounter(name);
    }

}

package org.jgroups.gossiprouter;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;

/**
 * Contains the node details.
 *
 * @since 5.1
 */
public class GossipRouterMember {

    private final PhysicalAddress physicalAddress;
    private final String logicalName;
    private final Address clientAddress; // address of the client which registered an item

    public GossipRouterMember(Address clientAddress, PhysicalAddress physicalAddress, String logicalName) {
        this.physicalAddress = physicalAddress;
        this.logicalName = logicalName;
        this.clientAddress = clientAddress;
    }

    public PhysicalAddress getPhysicalAddress() {
        return physicalAddress;
    }

    public String getLogicalName() {
        return logicalName;
    }

    public Address getClientAddress() {
        return clientAddress;
    }

    public void dump(StringBuilder sb) {
        sb.append(String.format("  %s: (client_address: %s, uuid:%s)\n", logicalName, physicalAddress, clientAddress));
    }

    @Override
    public String toString() {
        return String.format("client=%s, name=%s, address=%s", clientAddress, logicalName, physicalAddress);
    }
}

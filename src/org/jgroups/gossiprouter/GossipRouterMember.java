package org.jgroups.gossiprouter;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.gossiprouter.metrics.GossipRouterGroupMetrics;
import org.jgroups.gossiprouter.metrics.GossipRouterMemberMetrics;

/**
 * Contains the node details.
 *
 * @since 5.1
 */
public class GossipRouterMember {

    private final PhysicalAddress physicalAddress;
    private final String logicalName;
    private final Address clientAddress; // address of the client which registered an item
    private final GossipRouterMemberMetrics metrics;

    private GossipRouterMember(Address clientAddress, PhysicalAddress physicalAddress, String logicalName, GossipRouterMemberMetrics metrics) {
        this.physicalAddress = physicalAddress;
        this.logicalName = logicalName;
        this.clientAddress = clientAddress;
        this.metrics = metrics;
    }

    public static GossipRouterMember create(Address logicalAddress, Address clientAddress, PhysicalAddress physicalAddress, String logicalName, GossipRouterGroupMetrics metricsRegistry) {
        GossipRouterMemberMetrics metrics = metricsRegistry.registerMember(logicalAddress, physicalAddress, logicalName);
        return new GossipRouterMember(clientAddress, physicalAddress, logicalName, metrics);
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

    public void onUnicastMessageReceived(int bytes) {
        metrics.addUnicastMessageReceived(bytes);
    }

    public void onMulticastMessageReceived(int bytes) {
        metrics.addMulticastMessageReceived(bytes);
    }

    public void onUnicastMessageSent(int bytes) {
        metrics.addUnicastMessageSent(bytes);
    }

    public void onMulticastMessageSent(int bytes) {
        metrics.addMulticastMessageSent(bytes);
    }

    public void onDisconnect() {
        metrics.onDisconnect();
    }

    public void onUnregister() {
        metrics.unregister();
    }

    @Override
    public String toString() {
        return String.format("client=%s, name=%s, address=%s", clientAddress, logicalName, physicalAddress);
    }
}

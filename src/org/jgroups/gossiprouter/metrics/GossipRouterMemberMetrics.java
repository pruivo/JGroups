package org.jgroups.gossiprouter.metrics;

public interface GossipRouterMemberMetrics {
    void addUnicastMessageReceived(int bytes);

    void addMulticastMessageReceived(int bytes);

    void addUnicastMessageSent(int bytes);

    void addMulticastMessageSent(int bytes);

    void onDisconnect();

    void unregister();
}

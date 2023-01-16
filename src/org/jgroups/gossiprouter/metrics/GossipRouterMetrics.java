package org.jgroups.gossiprouter.metrics;

public interface GossipRouterMetrics {

    GossipRouterGroupMetrics createGroupMetrics(String name);
}

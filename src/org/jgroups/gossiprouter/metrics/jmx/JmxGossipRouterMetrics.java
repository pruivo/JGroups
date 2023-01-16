package org.jgroups.gossiprouter.metrics.jmx;

import org.jgroups.gossiprouter.metrics.GossipRouterGroupMetrics;
import org.jgroups.gossiprouter.metrics.GossipRouterMetrics;

import java.util.concurrent.ConcurrentHashMap;

public class JmxGossipRouterMetrics implements GossipRouterMetrics {

    private final ConcurrentHashMap<String, JmxGossipRouterGroupMetrics> groups = new ConcurrentHashMap<>(16);

    @Override
    public GossipRouterGroupMetrics createGroupMetrics(String name) {
        return groups.computeIfAbsent(name, JmxGossipRouterGroupMetrics::create);
    }
}

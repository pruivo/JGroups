package org.jgroups.gossiprouter.metrics.jmx;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.gossiprouter.metrics.GossipRouterGroupMetrics;
import org.jgroups.gossiprouter.metrics.GossipRouterMemberMetrics;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

@MBean
public class JmxGossipRouterGroupMetrics implements GossipRouterGroupMetrics {

    private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
    private final IntSupplier ZERO = () -> 0;

    private final String name;
    private final AtomicInteger registerEvents = new AtomicInteger(0);
    private final AtomicInteger unregisterEvents = new AtomicInteger(0);
    private final AtomicInteger suspectEvents = new AtomicInteger(0);
    private final ConcurrentHashMap<Address, JmxGossipRouterMemberMetrics> members = new ConcurrentHashMap<>(16);
    private IntSupplier groupSize = ZERO;

    public JmxGossipRouterGroupMetrics(String name) {
        this.name = name;
    }

    public static JmxGossipRouterGroupMetrics create(String name) {
        JmxGossipRouterGroupMetrics metrics = new JmxGossipRouterGroupMetrics(name);
        try {
            JmxConfigurator.register(metrics, Util.getMBeanServer(), "jgroups:name=GossipRouter,group=" + name);
        } catch (MalformedObjectNameException | MBeanRegistrationException e) {
            log.warn("Unable to register JMX bean for group %s", name);
        }
        return metrics;
    }

    private JmxGossipRouterMemberMetrics createMetrics(Address logicalAddress, PhysicalAddress physicalAddress, String logicalName) {
        return JmxGossipRouterMemberMetrics.create(logicalAddress, physicalAddress, logicalName, this).onConnect();
    }

    @Override
    public GossipRouterMemberMetrics registerMember(Address logicalAddress, PhysicalAddress physicalAddress, String logicalName) {
        return members.computeIfAbsent(logicalAddress, addr -> createMetrics(addr, physicalAddress, logicalName));
    }

    @Override
    public void setGroupSize(IntSupplier groupSizeSupplier) {
        groupSize = Objects.requireNonNull(groupSizeSupplier);
    }

    @Override
    public void incrementRegisterEvents() {
        registerEvents.incrementAndGet();
    }

    @Override
    public void incrementUnregisterEvents() {
        unregisterEvents.incrementAndGet();
    }

    @Override
    public void incrementSuspectEvents() {
        suspectEvents.incrementAndGet();
    }

    @ManagedAttribute
    public String getGroupName() {
        return name;
    }

    @ManagedAttribute
    public int getGroupSize() {
        return groupSize.getAsInt();
    }

    @ManagedAttribute
    public int getRegisterEvents() {
        return registerEvents.get();
    }

    @ManagedAttribute
    public int getUnregisterEvents() {
        return unregisterEvents.get();
    }

    @ManagedAttribute
    public int getSuspectEvents() {
        return suspectEvents.get();
    }

    public void removeMember(JmxGossipRouterMemberMetrics member) {
        members.remove(member.getLogicalAddress(), member);
    }
}

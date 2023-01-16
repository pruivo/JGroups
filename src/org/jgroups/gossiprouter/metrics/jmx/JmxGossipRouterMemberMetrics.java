package org.jgroups.gossiprouter.metrics.jmx;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.gossiprouter.metrics.GossipRouterMemberMetrics;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

@MBean
public class JmxGossipRouterMemberMetrics implements GossipRouterMemberMetrics {

    private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

    private final BytesMetric unicastReceived = new BytesMetric();
    private final BytesMetric unicastSent = new BytesMetric();
    private final BytesMetric multicastReceived = new BytesMetric();
    private final BytesMetric multicastSent = new BytesMetric();
    private final Address logicalAddress;
    private final Address physicalAddress;
    private final String logicalName;
    private final JmxGossipRouterGroupMetrics parentMetrics;
    private volatile boolean connected = true;

    private JmxGossipRouterMemberMetrics(Address logicalAddress, PhysicalAddress physicalAddress, String logicalName, JmxGossipRouterGroupMetrics parentMetrics) {
        this.logicalAddress = logicalAddress;
        this.physicalAddress = physicalAddress;
        this.logicalName = logicalName;
        this.parentMetrics = parentMetrics;
    }

    private static String jmxObjectName(String group, Address address) {
        return String.format("jgroups:name=GossipRouter,group=%s,node=%s", group, address);
    }

    public static JmxGossipRouterMemberMetrics create(Address logicalAddress, PhysicalAddress physicalAddress, String logicalName, JmxGossipRouterGroupMetrics parentMetrics) {
        JmxGossipRouterMemberMetrics metrics = new JmxGossipRouterMemberMetrics(logicalAddress, physicalAddress, logicalName, parentMetrics);
        try {
            JmxConfigurator.register(metrics, Util.getMBeanServer(), jmxObjectName(parentMetrics.getGroupName(), logicalAddress));
        } catch (MalformedObjectNameException | MBeanRegistrationException e) {
            log.warn("Unable to register JMX bean for member %s (group %s)", logicalAddress, parentMetrics.getGroupName());
        }
        return metrics;
    }

    @Override
    public void addUnicastMessageReceived(int bytes) {
        unicastReceived.add(bytes);
    }

    @Override
    public void addMulticastMessageReceived(int bytes) {
        multicastReceived.add(bytes);
    }

    @Override
    public void addUnicastMessageSent(int bytes) {
        unicastSent.add(bytes);
    }

    @Override
    public void addMulticastMessageSent(int bytes) {
        multicastSent.add(bytes);
    }

    @Override
    public void onDisconnect() {
        connected = false;
    }

    @Override
    public void unregister() {
        try {
            JmxConfigurator.unregister(Util.getMBeanServer(), jmxObjectName(parentMetrics.getGroupName(), logicalAddress));
        } catch (Exception e) {
            log.warn("Unable to unregister JMX bean for member %s (group %s)", logicalAddress, parentMetrics.getGroupName());
        }
        parentMetrics.removeMember(this);
    }

    public Address getLogicalAddress() {
        return logicalAddress;
    }

    public JmxGossipRouterMemberMetrics onConnect() {
        connected = true;
        return this;
    }

    @ManagedAttribute
    public String getAddress() {
        return String.valueOf(logicalAddress);
    }

    @ManagedAttribute
    public String getPhysicalAddress() {
        return String.valueOf(physicalAddress);
    }

    @ManagedAttribute
    public String getLogicalName() {
        return logicalName;
    }

    @ManagedAttribute
    public boolean isConnected() {
        return connected;
    }

    @ManagedAttribute
    public long getUnicastSentBytes() {
        return unicastSent.bytes.sum();
    }

    @ManagedAttribute
    public int getUnicastSent() {
        return unicastSent.count.get();
    }

    @ManagedAttribute
    public long getUnicastReceivedBytes() {
        return unicastReceived.bytes.sum();
    }

    @ManagedAttribute
    public int getUnicastReceived() {
        return unicastReceived.count.get();
    }

    @ManagedAttribute
    public long getMulticastSentBytes() {
        return multicastSent.bytes.sum();
    }

    @ManagedAttribute
    public int getMulticastSent() {
        return multicastSent.count.get();
    }

    @ManagedAttribute
    public long getMulticastReceivedBytes() {
        return multicastReceived.bytes.sum();
    }

    @ManagedAttribute
    public int getMulticastReceived() {
        return multicastReceived.count.get();
    }

    private static final class BytesMetric {
        final LongAdder bytes = new LongAdder();
        final AtomicInteger count = new AtomicInteger(0);

        void add(int bytes) {
            this.bytes.add(bytes);
            count.incrementAndGet();
        }
    }
}

package org.jgroups.gossiprouter;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.blocks.cs.BaseServer;
import org.jgroups.gossiprouter.metrics.GossipRouterGroupMetrics;
import org.jgroups.gossiprouter.metrics.GossipRouterMetrics;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.PingData;
import org.jgroups.stack.GossipData;
import org.jgroups.stack.GossipType;
import org.jgroups.util.ByteArrayDataOutputStream;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Represents a Gossip Router group.
 * <p>
 * A group is a cluster of members that communicate between them. This class keeps track of the members connect.
 *
 * @see 5.1
 */
public class GossipRouterGroup {

    private static final Log log = LogFactory.getLog(GossipRouterGroup.class);

    private final String name;
    private final BaseServer server;
    private final boolean printRegistrationToStdOut;
    private final ConcurrentHashMap<Address, GossipRouterMember> members = new ConcurrentHashMap<>(16);
    private final GossipRouterGroupMetrics metrics;

    public GossipRouterGroup(String name, BaseServer server, boolean printRegistrationToStdOut, GossipRouterMetrics metrics) {
        this.name = name;
        this.server = server;
        this.printRegistrationToStdOut = printRegistrationToStdOut;
        this.metrics = metrics.createGroupMetrics(name);
        this.metrics.setGroupSize(members::size);
    }

    private static PingData createPingData(Map.Entry<Address, GossipRouterMember> entry) {
        GossipRouterMember member = entry.getValue();
        return new PingData(entry.getKey(), true, member.getLogicalName(), member.getPhysicalAddress());
    }


    public void registerMember(Address logicalAddress, Address clientAddress, PhysicalAddress physicalAddress, String logicalName) {
        metrics.incrementRegisterEvents();
        GossipRouterMember m = GossipRouterMember.create(logicalAddress, clientAddress, physicalAddress, logicalName, metrics);
        members.put(logicalAddress, m);
        logRegistered(m);
    }

    public boolean unregisterMember(Address logicalAddress) {
        metrics.incrementUnregisterEvents();
        GossipRouterMember m = members.remove(logicalAddress);
        if (m != null) {
            m.onUnregister();
            logUnregistered(m);
        }
        return isEmpty();
    }

    public boolean onDisconnect(Address clientAddress, Consumer<Address> removedMember, boolean emitSuspectEvents) {
        Iterator<Map.Entry<Address, GossipRouterMember>> iterator = members.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Address, GossipRouterMember> entry = iterator.next();
            GossipRouterMember m = entry.getValue();
            if (!m.getClientAddress().equals(clientAddress)) {
                continue;
            }
            m.onDisconnect();
            iterator.remove();
            log.debug("connection to %s closed", clientAddress);
            logUnregistered(m);
            removedMember.accept(entry.getKey());
            if (emitSuspectEvents) {
                sendSuspect(entry.getKey());
            }
            return isEmpty();
        }
        return isEmpty();
    }

    public boolean isEmpty() {
        return members.isEmpty();
    }

    public int numberOfMembers() {
        return members.size();
    }

    public List<PingData> createPingData() {
        return members.entrySet().stream()
                .map(GossipRouterGroup::createPingData)
                .collect(Collectors.toList());
    }

    public void dumpMappings(StringBuilder sb) {
        sb.append(name).append(":\n");
        for (Map.Entry<Address, GossipRouterMember> entry : members.entrySet()) {
            GossipRouterMember member = entry.getValue();
            sb.append(String.format("  %s: %s (client_address: %s, uuid:%s)\n",
                    member.getLogicalName(),
                    member.getPhysicalAddress(),
                    member.getClientAddress(),
                    entry.getKey()));
        }
        members.values().forEach(m -> m.dump(sb));
    }

    private void sendSuspect(Address suspect) {
        metrics.incrementSuspectEvents();
        GossipData data = new GossipData(GossipType.SUSPECT, name, suspect);
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(data.serializedSize());
        try {
            data.writeTo(out);
        } catch (Exception ex) {
            log.error("failed marshalling gossip data %s: %s; dropping request", data, ex);
            return;
        }
        sendMulticast(null, out.buffer(), 0, out.position());
    }

    public void sendUnicast(Address src, Address dest, byte[] data, int offset, int length) {
        recordUnicastReceived(src, length);
        GossipRouterMember member = members.get(dest);
        if (member == null) {
            log.warn("dest %s in cluster %s not found", src, name);
            return;
        }
        member.onUnicastMessageSent(length);
        sendToMember(member, data, offset, length);
    }

    public void sendUnicast(Address src, Address dst, ByteBuffer buffer) {
        int length = buffer.remaining();
        recordUnicastReceived(src, length);
        GossipRouterMember member = members.get(dst);
        if (member == null) {
            log.warn("dest %s in cluster %s not found", dst, name);
            return;
        }
        member.onUnicastMessageSent(length);
        sendToMember(member, buffer);
    }

    public void sendMulticast(Address src, byte[] data, int offset, int length) {
        recordUnicastReceived(src, length);
        for (GossipRouterMember member : members.values()) {
            member.onMulticastMessageSent(length);
            sendToMember(member, data, offset, length);
        }
    }

    public void sendMulticast(Address src, ByteBuffer buffer) {
        int length = buffer.remaining();
        recordMulticastReceived(src, length);
        for (GossipRouterMember member : members.values()) {
            member.onMulticastMessageSent(length);
            sendToMember(member, buffer.duplicate());
        }
    }

    private void recordUnicastReceived(Address src, int length) {
        GossipRouterMember sender = getSender(src);
        if (sender != null) {
            sender.onUnicastMessageReceived(length);
        }
    }

    private void recordMulticastReceived(Address src, int length) {
        GossipRouterMember sender = getSender(src);
        if (sender != null) {
            sender.onMulticastMessageReceived(length);
        }
    }

    private GossipRouterMember getSender(Address src) {
        return src == null ? null : members.get(src);
    }

    private void logRegistered(GossipRouterMember m) {
        if (log.isDebugEnabled())
            log.debug("added %s (%s) to group %s", m.getLogicalName(), m.getPhysicalAddress(), name);
        if (printRegistrationToStdOut)
            System.out.printf("added %s (%s) to group %s\n", m.getLogicalName(), m.getPhysicalAddress(), name);
    }

    private void logUnregistered(GossipRouterMember m) {
        if (log.isDebugEnabled())
            log.debug("removed %s (%s) from group %s", m.getLogicalName(), m.getPhysicalAddress(), name);
        if (printRegistrationToStdOut)
            System.out.printf("removed %s (%s) from group %s\n", m.getLogicalName(), m.getPhysicalAddress(), name);
    }

    private void sendToMember(GossipRouterMember member, byte[] data, int offset, int length) {
        try {
            server.send(member.getClientAddress(), data, offset, length);
        } catch (Exception ex) {
            log.error("failed sending unicast message to %s: %s", member.getClientAddress(), ex);
        }
    }

    private void sendToMember(GossipRouterMember member, ByteBuffer buffer) {
        try {
            server.send(member.getClientAddress(), buffer);
        } catch (Exception ex) {
            log.error("failed sending unicast message to %s: %s", member.getClientAddress(), ex);
        }
    }
}

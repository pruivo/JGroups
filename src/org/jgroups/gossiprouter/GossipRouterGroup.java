package org.jgroups.gossiprouter;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.blocks.cs.BaseServer;
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

    public GossipRouterGroup(String name, BaseServer server, boolean printRegistrationToStdOut) {
        this.name = name;
        this.server = server;
        this.printRegistrationToStdOut = printRegistrationToStdOut;
    }

    private static PingData createPingData(Map.Entry<Address, GossipRouterMember> entry) {
        GossipRouterMember member = entry.getValue();
        return new PingData(entry.getKey(), true, member.getLogicalName(), member.getPhysicalAddress());
    }


    public void registerMember(Address logicalAddress, Address clientAddress, PhysicalAddress physicalAddress, String logicalName) {
        GossipRouterMember m = new GossipRouterMember(clientAddress, physicalAddress, logicalName);
        members.put(logicalAddress, m);
        logRegistered(m);
    }

    public boolean unregisterMember(Address logicalAddress) {
        GossipRouterMember m = members.remove(logicalAddress);
        if (m != null) {
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
        GossipData data = new GossipData(GossipType.SUSPECT, name, suspect);
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(data.serializedSize());
        try {
            data.writeTo(out);
        } catch (Exception ex) {
            log.error("failed marshalling gossip data %s: %s; dropping request", data, ex);
            return;
        }
        sendMulticast(out.buffer(), 0, out.position());
    }

    public void sendUnicast(Address logicalAddress, byte[] data, int offset, int length) {
        GossipRouterMember member = members.get(logicalAddress);
        if (member == null) {
            log.warn("dest %s in cluster %s not found", logicalAddress, name);
            return;
        }
        sendToMember(member, data, offset, length);
    }

    public void sendUnicast(Address logicalAddress, ByteBuffer buffer) {
        GossipRouterMember member = members.get(logicalAddress);
        if (member == null) {
            log.warn("dest %s in cluster %s not found", logicalAddress, name);
            return;
        }
        sendToMember(member, buffer);
    }

    public void sendMulticast(byte[] data, int offset, int length) {
        for (GossipRouterMember member : members.values()) {
            sendToMember(member, data, offset, length);
        }
    }

    public void sendMulticast(ByteBuffer buffer) {
        for (GossipRouterMember member : members.values()) {
            sendToMember(member, buffer.duplicate());
        }
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

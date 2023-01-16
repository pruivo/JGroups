package org.jgroups.gossiprouter.metrics;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;

import java.util.function.IntSupplier;

public interface GossipRouterGroupMetrics {

    GossipRouterMemberMetrics registerMember(Address logicalAddress, PhysicalAddress physicalAddress, String logicalName);

    void setGroupSize(IntSupplier groupSizeSupplier);

    void incrementRegisterEvents();

    void incrementUnregisterEvents();

    void incrementSuspectEvents();
}

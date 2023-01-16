package org.jgroups.gossiprouter.metrics;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;

import java.util.function.IntSupplier;

public final class NoOpGossipRouterMetrics implements GossipRouterMetrics {

    public static final NoOpGossipRouterMetrics INSTANCE = new NoOpGossipRouterMetrics();
    private static final GossipRouterMemberMetrics MEMBER_METRICS = new GossipRouterMemberMetrics() {
        @Override
        public void addUnicastMessageReceived(int bytes) {

        }

        @Override
        public void addMulticastMessageReceived(int bytes) {

        }

        @Override
        public void addUnicastMessageSent(int bytes) {

        }

        @Override
        public void addMulticastMessageSent(int bytes) {

        }

        @Override
        public void onDisconnect() {

        }

        @Override
        public void unregister() {

        }
    };
    private static final GossipRouterGroupMetrics GROUP_METRICS = new GossipRouterGroupMetrics() {
        @Override
        public GossipRouterMemberMetrics registerMember(Address logicalAddress, PhysicalAddress physicalAddress, String logicalName) {
            return MEMBER_METRICS;
        }

        @Override
        public void setGroupSize(IntSupplier groupSizeSupplier) {

        }

        @Override
        public void incrementRegisterEvents() {

        }

        @Override
        public void incrementUnregisterEvents() {

        }

        @Override
        public void incrementSuspectEvents() {

        }
    };

    private NoOpGossipRouterMetrics() {
    }

    @Override
    public GossipRouterGroupMetrics createGroupMetrics(String name) {
        return GROUP_METRICS;
    }

}

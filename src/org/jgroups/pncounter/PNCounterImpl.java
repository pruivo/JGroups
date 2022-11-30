package org.jgroups.pncounter;

import org.jgroups.Address;
import org.jgroups.blocks.pncounter.PNCounter;
import org.jgroups.util.CompletableFutures;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO! document this
 */
public class PNCounterImpl implements PNCounter {

    private final String name;
    private final PNCounterProtocol protocol;
    private final Address local;
    private final Map<Address, PNCounterState> counter;

    private static PNCounterState createPNCounterData(Address ignored) {
        return new PNCounterState();
    }

    public PNCounterImpl(String name, Address localAddress, PNCounterProtocol protocol) {
        this.name = name;
        this.protocol = protocol;
        this.local = localAddress;
        this.counter = new ConcurrentHashMap<>();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long get() {
        return counter.values().stream().map(PNCounterState::sum).reduce(0L, Long::sum);
    }

    @Override
    public CompletionStage<Void> add(long value) {
        if (value == 0) {
            return CompletableFutures.completedNull();
        }
        PNCounterState data = counter.computeIfAbsent(local, PNCounterImpl::createPNCounterData);
        PNCounterStateSnapshot snapshot = data.add(value);
        return protocol.updateAllMembers(name, snapshot);
    }

    public void onUpdate(Address originator, PNCounterStateSnapshot snapshot) {
        PNCounterState data = counter.computeIfAbsent(originator, PNCounterImpl::createPNCounterData);
        data.update(snapshot);
    }

    public PNCounterSnapshot snapshot() {
        Map<Address, PNCounterStateSnapshot> snapshotMap = new HashMap<>();
        for (Map.Entry<Address, PNCounterState> entry : counter.entrySet()) {
            snapshotMap.put(entry.getKey(), entry.getValue().snapshot());
        }
        return new PNCounterSnapshot(snapshotMap);
    }

    public void applySnapshot(PNCounterSnapshot snapshot) {
        for (Map.Entry<Address, PNCounterStateSnapshot> entry : snapshot.getSnapshot().entrySet()) {
            counter.computeIfAbsent(entry.getKey(), PNCounterImpl::createPNCounterData).update(entry.getValue());
        }
    }

}

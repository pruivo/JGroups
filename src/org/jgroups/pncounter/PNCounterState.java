package org.jgroups.pncounter;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class PNCounterState {

    private static final AtomicLongFieldUpdater<PNCounterState> P_UPDATER = AtomicLongFieldUpdater.newUpdater(PNCounterState.class, "pCounter");
    private static final AtomicLongFieldUpdater<PNCounterState> N_UPDATER = AtomicLongFieldUpdater.newUpdater(PNCounterState.class, "nCounter");

    volatile long pCounter;
    volatile long nCounter;

    public long sum() {
        return pCounter - nCounter;
    }

    public PNCounterStateSnapshot add(long value) {
        if (value >= 0) {
            P_UPDATER.addAndGet(this, value);
        } else {
            N_UPDATER.addAndGet(this, -value);
        }
        return snapshot();
    }

    public void update(PNCounterStateSnapshot snapshot) {
        P_UPDATER.accumulateAndGet(this, snapshot.getPositiveCounter(), Long::max);
        N_UPDATER.accumulateAndGet(this, snapshot.getNegativeCounter(), Long::max);
    }

    public PNCounterStateSnapshot snapshot() {
        return new PNCounterStateSnapshot(pCounter, nCounter);
    }
}

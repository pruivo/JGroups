package org.jgroups.pncounter;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class PNCounterData {

    private static final AtomicLongFieldUpdater<PNCounterData> P_UPDATER = AtomicLongFieldUpdater.newUpdater(PNCounterData.class, "pCounter");
    private static final AtomicLongFieldUpdater<PNCounterData> N_UPDATER = AtomicLongFieldUpdater.newUpdater(PNCounterData.class, "nCounter");

    volatile long pCounter;
    volatile long nCounter;

    public long sum() {
        return pCounter - nCounter;
    }

    public PNCounterSnapshot add(long value) {
        if (value >= 0) {
            P_UPDATER.addAndGet(this, value);
        } else {
            N_UPDATER.addAndGet(this, -value);
        }
        return snapshot();
    }

    public void update(PNCounterSnapshot snapshot) {
        P_UPDATER.accumulateAndGet(this, snapshot.getPositiveCounter(), Long::max);
        N_UPDATER.accumulateAndGet(this, snapshot.getNegativeCounter(), Long::max);
    }

    public PNCounterSnapshot snapshot() {
        return new PNCounterSnapshot(pCounter, nCounter);
    }
}

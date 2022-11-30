package org.jgroups.pncounter;

import org.jgroups.Address;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO! document this
 */
public class PNCounterSnapshot implements SizeStreamable {
    private Map<Address, PNCounterStateSnapshot> snapshot;

    public PNCounterSnapshot(Map<Address, PNCounterStateSnapshot> snapshot) {
        this.snapshot = snapshot;
    }

    public Map<Address, PNCounterStateSnapshot> getSnapshot() {
        return snapshot;
    }

    @Override
    public int serializedSize() {
        int numberOfEntries = snapshot.size();
        int size = snapshot.entrySet().stream()
                .map(PNCounterSnapshot::serializedSizeOf)
                .reduce(0, Integer::sum);
        return Bits.size(numberOfEntries) + size;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeIntCompressed(snapshot.size(), out);
        for (Map.Entry<Address, PNCounterStateSnapshot> entry : snapshot.entrySet()) {
            Util.writeAddress(entry.getKey(), out);
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int numberOfEntries = Bits.readIntCompressed(in);
        this.snapshot = new HashMap<>();
        for (int i = 0; i < numberOfEntries; ++i) {
            Address address = Util.readAddress(in);
            PNCounterStateSnapshot snapshot = new PNCounterStateSnapshot();
            snapshot.readFrom(in);
            this.snapshot.put(address, snapshot);
        }
    }

    public static int serializedSizeOf(Map.Entry<Address, PNCounterStateSnapshot> entry) {
        return Util.size(entry.getKey()) + entry.getValue().serializedSize();
    }
}

package org.jgroups.pncounter;

import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class PNCounterSnapshot implements SizeStreamable {

    private long pCounter;
    private long nCounter;

    public PNCounterSnapshot() {
    }

    public PNCounterSnapshot(long pCounter, long nCounter) {
        this.pCounter = pCounter;
        this.nCounter = nCounter;
    }

    public long getPositiveCounter() {
        return pCounter;
    }

    public long getNegativeCounter() {
        return nCounter;
    }

    public long sum() {
        return pCounter - nCounter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PNCounterSnapshot that = (PNCounterSnapshot) o;
        return pCounter == that.pCounter && nCounter == that.nCounter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pCounter, nCounter);
    }

    @Override
    public int serializedSize() {
        return Bits.size(pCounter) + Bits.size(nCounter);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeLongCompressed(pCounter, out);
        Bits.writeLongCompressed(nCounter, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        pCounter = Bits.readLongCompressed(in);
        nCounter = Bits.readLongCompressed(in);
    }
}

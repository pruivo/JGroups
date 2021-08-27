package org.jgroups.pncounter;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class PNCounterHeader extends Header {

    private long reqId;
    private String counterName;

    PNCounterHeader() {
    }

    public PNCounterHeader(long reqId, String counterName) {
        this.reqId = reqId;
        this.counterName = counterName;
    }

    public long getReqId() {
        return reqId;
    }

    public String getCounterName() {
        return counterName;
    }

    @Override
    public Supplier<? extends Header> create() {
        return PNCounterHeader::new;
    }

    @Override
    public short getMagicId() {
        return 95;
    }

    @Override
    public int serializedSize() {
        return Bits.size(reqId) + Bits.sizeUTF(counterName);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeLongCompressed(reqId, out);
        out.writeUTF(counterName);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        this.reqId = Bits.readLongCompressed(in);
        this.counterName = in.readUTF();
    }

    public PNCounterHeader ack() {
        return new PNCounterHeader(reqId, null);
    }
}

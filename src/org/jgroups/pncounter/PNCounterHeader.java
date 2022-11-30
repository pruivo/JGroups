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

    public static final byte STATE = 0;
    public static final byte UPDATE = 1;
    public static final byte ACK = 2;

    private byte type;
    private long reqId;
    private String counterName;

    private PNCounterHeader() {
    }

    public static PNCounterHeader stateHeader(String counterName) {
        return new PNCounterHeader(STATE, 0, counterName);
    }

    public static PNCounterHeader updateHeader(long reqId, String counterName) {
        return new PNCounterHeader(UPDATE, reqId, counterName);
    }

    private PNCounterHeader(byte type, long reqId, String counterName) {
        this.type = type;
        this.reqId = reqId;
        this.counterName = counterName;
    }

    public long getReqId() {
        return reqId;
    }

    public String getCounterName() {
        return counterName;
    }

    public byte getType() {
        return type;
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
        return Global.BYTE_SIZE + Bits.size(reqId) + Bits.sizeUTF(counterName);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type);
        Bits.writeLongCompressed(reqId, out);
        out.writeUTF(counterName);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        this.type = in.readByte();
        this.reqId = Bits.readLongCompressed(in);
        this.counterName = in.readUTF();
    }

    public PNCounterHeader ack() {
        return new PNCounterHeader(ACK, reqId, null);
    }

    @Override
    public String toString() {
        return "PNCounterHeader{" +
                "type=" + type +
                ", reqId=" + reqId +
                ", counterName='" + counterName + '\'' +
                '}';
    }
}

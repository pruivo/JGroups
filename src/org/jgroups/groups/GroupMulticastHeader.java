package org.jgroups.groups;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Pedro
 */
public class GroupMulticastHeader extends Header {

    public static final byte UNKNOWN = 0x00; //0000|0000
    private static final byte FLAGS_MASK = (byte) 0xF0;
    private static final byte TYPE_MASK = 0x0F;

    //type
    public static final byte MESSAGE = 0x01; //0000|0001
    public static final byte MESSAGE_FINAL = 0x02; //0000|0010
    public static final byte MESSAGE_PROPOSE = 0x03; //0000|0011
    public static final byte COORDINATOR_REQUEST = 0x04; //0000|0100
    public static final byte COORDINATOR_RESPONSE = 0x05; //0000|0101
    public static final byte BUNDLE_MESSAGE = 0x06; //0000|0110
    public static final byte MESSAGE_WITH_PROPOSE = 0x07; //0000|0111
    public static final byte PROPOSE = 0x08; //0000|1000

    //flag
    private static final byte ONE_DESTINATION = 0x10; //0001|0000

    private Address origin; //in coordinator request/response, this var contains the failed node
    private MessageID messageID; //address and sequence number
    private long timestamp = -1L;
    private long seqNo;

    //4 bits for flags, other 4 bits for type: 4 flags and 15 types
    private byte flags = UNKNOWN;
    private Set<Address> group = new HashSet<Address>();

    public GroupMulticastHeader() {}

    public GroupMulticastHeader(Address origin, long msgID) {
        this.origin = origin;
        this.messageID = new MessageID(origin, msgID);
    }

    public GroupMulticastHeader(Address addr) {
        this.origin = addr;
    }

    public byte getType() {
        return (byte) (flags & TYPE_MASK);
    }

    public void setType(byte type) {
        this.flags = (byte) ((flags & FLAGS_MASK) | (type & TYPE_MASK));
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long newTimestamp) {
        timestamp = newTimestamp;
    }

    public MessageID getID() {
        return messageID;
    }

    public Address getOrigin() {
        return origin;
    }

    public void setOrigin(Address origin) {
        this.origin = origin;
    }

    public void setOneDestinationFlag() {
        flags |= ONE_DESTINATION;
    }

    public boolean isOneDestinationFlagSet() {
        return (flags & ONE_DESTINATION) != 0;
    }

    public void addDestinations(Collection<Address> addrs) {
        group.addAll(addrs);
    }

    public Set<Address> getDestinations() {
        return group;
    }

    public GroupMulticastHeader copy() {
        GroupMulticastHeader hdr = new GroupMulticastHeader();
        hdr.origin = this.origin;
        hdr.messageID = this.messageID.copy();
        hdr.timestamp = this.timestamp;
        hdr.flags = this.flags;
        hdr.group = new HashSet<Address>(this.group);
        return hdr;
    }

    @Override
    public int size() {
        int size = Util.size(timestamp) + Global.BYTE_SIZE + Util.size(messageID.getId()) + Global.INT_SIZE +
                Util.size(origin);
        for (Address address : group) {
            size += Util.size(address);
        }
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[Message ID: ").append(messageID);
        sb.append(",timestamp: ").append(timestamp);
        sb.append(",origin address: ").append(origin);
        sb.append("]");
        return sb.toString();
    }

    public long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public static String type2String(byte type) {
        type &= TYPE_MASK;
        switch(type) {
            case MESSAGE: return "MESSAGE";
            case MESSAGE_FINAL: return "MESSAGE_FINAL";
            case MESSAGE_PROPOSE: return"MESSAGE_PROPOSE";
            case COORDINATOR_REQUEST: return "COORDINATOR_REQUEST";
            case COORDINATOR_RESPONSE: return "COORDINATOR_RESPONSE";
            case BUNDLE_MESSAGE: return "BUNDLE_MESSAGE";
            case MESSAGE_WITH_PROPOSE: return "MESSAGE_WITH_PROPOSE";
            case PROPOSE: return "PROPOSE";
            default: return "UNKNOWN_TYPE";
        }
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeLong(timestamp, out);
        out.writeByte(flags);
        Util.writeAddress(origin, out);
        Util.writeLong(messageID.getId(), out);
        out.writeInt(group.size());
        for(Address address : group) {
            Util.writeAddress(address, out);
        }
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        timestamp = Util.readLong(in);
        flags = in.readByte();
        origin = Util.readAddress(in);
        long id = Util.readLong(in);
        messageID = new MessageID(origin, id);
        for (int i = in.readInt(); i > 0; --i) {
            group.add(Util.readAddress(in));
        }
    }
}


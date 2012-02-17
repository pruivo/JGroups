package org.jgroups.groups;

import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;

/**
 * @author pedro
 * Date: 06-01-2011
 *
 * equals to ViewId
 */
public class MessageID implements Externalizable, Comparable, Cloneable, Streamable {
    private Address address = null;
    private long id = 0;

    public MessageID() {}

    public MessageID(Address address, long id) {
        this.address = address;
        this.id = id;
    }

    public MessageID(Address address) {
        this.address = address;
    }

    public void setID(long id) {
        this.id = id;
    }

    @Override
    public int compareTo(Object other) {
        if(other == null) return 1;

        if(!(other instanceof MessageID)) {
            throw new ClassCastException("MessageID.compareTo(): message id is not comparable with different Objects");
        }

        MessageID otherID = (MessageID) other;

        if(this.getId() < otherID.getId()){
            return -1;
        } else if(this.getId() > otherID.getId()){
            return 1;
        }

        return this.address.compareTo(otherID.address);
    }

    public MessageID copy() {
        return (MessageID) clone();
    }

    public long getId() {
        return id;
    }

    public Address getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return "MessageID{" + address + ":" + id + "}";
    }

    public Object clone() {
        return new MessageID(address, id);
    }

    public int compare(Object o) {
        return compareTo(o);
    }


    public boolean equals(Object other) {
        return (other instanceof MessageID) && compareTo(other) == 0;
    }


    public int hashCode() {
        return (int)id;
    }


    public int serializedSize() {
        return Util.size(id) + Util.size(address);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(address, out);
        Util.writeLong(id, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        address = Util.readAddress(in);
        id = Util.readLong(in);
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        try {
            writeTo(objectOutput);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        try {
            readFrom(objectInput);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}

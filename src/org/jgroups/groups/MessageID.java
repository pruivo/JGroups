package org.jgroups.groups;

import org.jgroups.Address;
import org.jgroups.Global;
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
    private Address addr = null;
    private long id = 0;

    public MessageID() {
    }

    public MessageID(Address addr, long id) {
        this.addr = addr;
        this.id = id;
    }

    public MessageID(Address addr) {
        this.addr = addr;
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

        return this.addr.compareTo(otherID.addr);
    }

    public MessageID copy() {
        return (MessageID) clone();
    }

    public long getId() {
        return id;
    }

    public Address getAddress() {
        return addr;
    }

    @Override
    public String toString() {
        return "[MessageID:" + addr + ":" + id + ']';
    }

    public Object clone() {
        return new MessageID(addr, id);
    }

    public int compare(Object o) {
        return compareTo(o);
    }


    public boolean equals(Object other) {
        return compareTo(other) == 0;
    }


    public int hashCode() {
        return (int)id;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(addr);
        out.writeLong(id);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        addr=(Address)in.readObject();
        id=in.readLong();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeAddress(addr, out);
        out.writeLong(id);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        addr=Util.readAddress(in);
        id=in.readLong();
    }

    public int serializedSize() {
        int retval= Global.LONG_SIZE; // for the id
        retval+=Util.size(addr);
        return retval;
    }
}

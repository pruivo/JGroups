package org.jgroups.groups;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;

/**
 * @author Pedro
 */
public class GroupAddress implements Address {
    private Set<Address> destinations;

    public GroupAddress() {
        destinations = new HashSet<Address>();
    }

    public void addAddress(Address addr) {
        destinations.add(addr);
    }

    public void addAllAddress(Collection<Address> addrs) {
        destinations.addAll(addrs);
    }

    public Set<Address> getAddresses() {
        return destinations;
    }

    public boolean isMulticastAddress() {
        return false;
    }

    public boolean isGroupAddress() {
        return true;
    }

    public int size() {
        int size = Global.INT_SIZE;
        for(Address addr : destinations) {
            size += Util.size(addr);
        }
        return size;
    }

    @Override
    public String toString() {
        return "GroupAddress=(" + destinations + ")";
    }

    @Override
    public int hashCode() {
        int hc = 0;
        for(Address addr : destinations) {
            hc += addr.hashCode();
        }
        return hc;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(!(obj instanceof GroupAddress)) {
            return false;
        }

        GroupAddress other = (GroupAddress) obj;

        return other == this || (this.destinations.containsAll(other.destinations) &&
                other.destinations.containsAll(this.destinations));
    }

    public int compareTo(Address o) {
        int hc1, hc2;

        if(this == o) return 0;
        if(!(o instanceof GroupAddress))
            throw new ClassCastException("comparison between different classes: the other object is " +
                    (o != null? o.getClass() : o));
        GroupAddress other = (GroupAddress) o;

        hc1 = this.hashCode();
        hc2 = other.hashCode();

        if(hc1 == hc2) {
            return this.destinations.size() < other.destinations.size() ? -1 :
                    this.destinations.size() > other.destinations.size() ? 1 : 0;
        } else {
            return hc1 < hc2 ? -1 : 1;
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(destinations.size());
        for(Address addr : destinations) {
            out.writeObject(addr);
        }

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for( ; size > 0; --size) {
            destinations.add((Address) in.readObject());
        }
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeAddresses(destinations, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        destinations = (Set<Address>) Util.readAddresses(in, HashSet.class);
    }
}
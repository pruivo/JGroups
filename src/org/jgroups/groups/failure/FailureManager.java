package org.jgroups.groups.failure;

import org.jgroups.Address;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Pedro
 */
public class FailureManager {
    //coordinator info
    private final ConcurrentMap<Address, Info> msgsToOrder;

    //all nodes info
    private final ConcurrentSkipListSet<Address> failedNodes;

    public FailureManager() {
        this.msgsToOrder = new ConcurrentHashMap<Address, Info>();
        this.failedNodes = new ConcurrentSkipListSet<Address>();
    }

    /* ----- for all nodes ------ */

    public void addAllFailedNode(Collection<Address> members) {
        for(Address addr : members) {
            failedNodes.add(addr);
        }
    }

    public void removeFailedNode(Address member) {
        failedNodes.remove(member);
    }

    public Set<Address> getFailedNodes() {
        return new HashSet<Address>(failedNodes);
    }

    public boolean isFailedNodeListEmpty() {
        return failedNodes.isEmpty();
    }

    /* -------- for coordinator ----- */

    public void processMember(Address from, Address failedNode, Map<Long, Long> delivered){
        Info info = new Info();
        Info existing = msgsToOrder.putIfAbsent(failedNode, info);
        if(existing != null) {
            info = existing;
        }
        info.processMessage(from, delivered);
    }

    public boolean isOK(Address failedNode) {
        Info info = msgsToOrder.get(failedNode);
        return info != null && info.isOrdered();
    }

    public Map<Long, Long> getFinalTimestamp(Address failedNode) {
        Info info = msgsToOrder.get(failedNode);
        return info != null ? info.getMessages() : Collections.<Long, Long>emptyMap();
    }

    public void removeFailedMember(Address failedNode) {
        msgsToOrder.remove(failedNode);
    }

    public void handleFailure(Set<Address> addrs, List<Address> members) {
        for(Info i : msgsToOrder.values()) {
            i.addFailedMember(addrs);
        }
        for(Address addr : addrs) {
            Info info = new Info();
            Info existing = msgsToOrder.putIfAbsent(addr, info);
            if(existing == null) {
                info.setAliveMembers(members);
            }
        }
    }

    /* ----- auxiliary classes ----- */

    private class Info {
        private final ConcurrentMap<Long, Long> delTimestamp;
        private final ConcurrentSkipListSet<Address> acksInMiss;

        public Info() {
            this.delTimestamp = new ConcurrentHashMap<Long, Long>();
            this.acksInMiss = new ConcurrentSkipListSet<Address>();
        }

        public void setAliveMembers(Collection<Address> members) {
            if(acksInMiss.isEmpty()) {
                acksInMiss.addAll(members);
            }
        }

        public void processMessage(Address from, Map<Long, Long> delivered) {
            acksInMiss.remove(from);
            for(Map.Entry<Long, Long> entry : delivered.entrySet()) {
                delTimestamp.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        public void addFailedMember(Set<Address> members) {
            acksInMiss.addAll(members);
        }

        public boolean isOrdered() {
            return acksInMiss.isEmpty();
        }

        public Map<Long, Long> getMessages() {
            return new HashMap(delTimestamp);
        }
    }
}

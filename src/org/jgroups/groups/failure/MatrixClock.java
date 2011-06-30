package org.jgroups.groups.failure;

import org.jgroups.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author pedro
 *         Date: 04-05-2011
 */
public class MatrixClock {
    private final Map<Address, Map<Address, Long>> matrixClock = new HashMap<Address, Map<Address, Long>>();
    private final ReentrantLock lock = new ReentrantLock();

    public MatrixClock() {}

    public void seeMessage(Address myAddress, Address from, long seqno) {
        try {
            lock.lock();
            if(matrixClock.containsKey(myAddress)) {
                Map<Address, Long> myVectorClock = matrixClock.get(myAddress);
                myVectorClock.put(from, seqno);
            } else {
                Map<Address, Long> myVectorClock = new HashMap<Address, Long>();
                myVectorClock.put(from, seqno);
                matrixClock.put(myAddress, myVectorClock);
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateVectorClock(Address from, Map<Address, Long> vectorClock) {
        try {
            lock.lock();
            matrixClock.put(from, vectorClock);
        } finally {
            lock.unlock();
        }
    }

    public long lastMessageSeenFrom(Address from) {
        long lastSeqno = Long.MAX_VALUE;
        try {
            lock.lock();
            for(Map.Entry<Address, Map<Address, Long>> vectorClock : matrixClock.entrySet()) {
                Long seqno = vectorClock.getValue().get(from);
                if(seqno == null) {
                    return 0;
                } else if(seqno < lastSeqno) {
                    lastSeqno = seqno;
                }
            }
        } finally {
            lock.unlock();
        }
        return lastSeqno;
    }

}

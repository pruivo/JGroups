package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * TODO! document this
 */
@MBean(description="Simple flow control protocol based on a credit system")
public class EUFC extends FlowControl {

    private static final AtomicLongFieldUpdater<MemberCredits> CREDITS_UPDATER = AtomicLongFieldUpdater.newUpdater(MemberCredits.class, "credits");

    private final Map<Address, MemberCredits> sentCredits = new ConcurrentHashMap<>();

    @Override
    public int getNumberOfBlockings() {
        return 0;
    }

    @Override
    public double getAverageTimeBlocked() {
        return 0;
    }

    @Override
    public String printSenderCredits() {
        return null;
    }

    @Override
    protected boolean handleMulticastMessage() {
        return false;
    }

    @Override
    protected Header getReplenishHeader() {
        return UFC.UFC_REPLENISH_HDR;
    }

    @Override
    protected Header getCreditRequestHeader() {
        return UFC.UFC_CREDIT_REQUEST_HDR;
    }

    @Override
    protected void handleCredit(Address sender, long increase) {
        MemberCredits credits = sentCredits.get(sender);
        if (credits == null) {
            return;
        }
        credits.increment(increase);
        sendUnblockEvent(sender);
    }

    @Override
    protected Object handleDownMessage(Message msg, int length) {
        Address dst = msg.getDest();
        if (!running || dst == null) {
            return down_prot.down(msg);
        }
        MemberCredits credits = sentCredits.get(dst);
        if (credits == null) {
            return down_prot.down(msg);
        }
        if (credits.tryDecrement(length)) {
            return down_prot.down(msg);
        }
        sendBlockEvent(msg);
        return null;
    }

    @Override
    protected void handleViewChange(List<Address> mbrs) {
        super.handleViewChange(mbrs);
        Set<Address> membersSet = new HashSet<>(mbrs);
        List<Address> removedMembers = new ArrayList<>(sentCredits.size());
        for (Address member : sentCredits.keySet()) {
            if (membersSet.contains(member)) {
                membersSet.remove(member);
                continue;
            }
            sentCredits.remove(member);
            removedMembers.add(member);
        }
        for (Address member : membersSet) {
            sentCredits.putIfAbsent(member, new MemberCredits(max_credits));
        }
        removedMembers.forEach(this::sendUnblockEvent);
    }

    private void sendUnblockEvent(Address destination) {
        up_prot.up(new Event(Event.CREDITS_AVAILABLE, destination));
    }

    private void sendBlockEvent(Message message) {
        up_prot.up(new Event(Event.NO_CREDITS, message));
    }

    private static class MemberCredits {
        volatile long credits;

        MemberCredits(long initialCredits) {
            credits = initialCredits;
        }

        boolean tryDecrement(long amount) {
            long remaining;
            do {
                remaining = credits;
                if (credits < amount) {
                    return false;
                }
            } while (!CREDITS_UPDATER.compareAndSet(this, remaining, remaining - amount));
            return true;
        }

        void increment(long amount) {
            long remaining;
            do {
                remaining = credits;
            } while (!CREDITS_UPDATER.compareAndSet(this, remaining, remaining + amount));
        }
    }
}

package org.jgroups.pncounter;

import org.jgroups.Address;
import org.jgroups.annotations.GuardedBy;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class Request {

    private final Set<Address> membersToAck;
    private final long requestId;
    @GuardedBy("this")
    private int acksMissing;
    private final CompletableFuture<Void> completableFuture;

    public Request(Collection<Address> membersToAck, long requestId, int acksMissing) {
        this.membersToAck = new HashSet<>(membersToAck);
        this.requestId = requestId;
        this.acksMissing = acksMissing;
        this.completableFuture = new CompletableFuture<>();
    }

    public CompletionStage<Void> toCompletionStage() {
        return completableFuture;
    }

    public boolean onAck(Address src) {
        boolean done = false;
        synchronized (this) {
            if (membersToAck.remove(src)) {
                done = membersToAck.isEmpty() || --acksMissing <= 0;
            }
        }
        if (done) {
            completableFuture.complete(null);
        }
        return done;
    }

    public long getRequestId() {
        return requestId;
    }
}

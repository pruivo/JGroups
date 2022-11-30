package org.jgroups.pncounter;

import org.jgroups.Address;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class RequestRepository {

    private static final AtomicLong REQ_ID_GENERATOR = new AtomicLong();

    private final ConcurrentHashMap<Long, Request> requests;

    public RequestRepository() {
        requests = new ConcurrentHashMap<>();
    }

    public Request createRequest(Collection<Address> members, int acksRequired) {
        long reqId = REQ_ID_GENERATOR.incrementAndGet();
        Request request = new Request(members, reqId, acksRequired);
        requests.put(reqId, request);
        return request;
    }

    public void ack(long requestId, Address src) {
        Request request = requests.get(requestId);
        if (request == null) {
            return;
        }
        if (request.onAck(src)) {
            requests.remove(requestId);
        }
    }

}

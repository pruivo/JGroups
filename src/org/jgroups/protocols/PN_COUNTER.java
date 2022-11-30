package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.EmptyMessage;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.pncounter.PNCounter;
import org.jgroups.pncounter.PNCounterHeader;
import org.jgroups.pncounter.PNCounterImpl;
import org.jgroups.pncounter.PNCounterProtocol;
import org.jgroups.pncounter.PNCounterSnapshot;
import org.jgroups.pncounter.PNCounterStateSnapshot;
import org.jgroups.pncounter.Request;
import org.jgroups.pncounter.RequestRepository;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ByteArray;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 5.1
 */
@MBean(description = "Protocol to maintain positive-negative counters")
public class PN_COUNTER extends Protocol implements PNCounterProtocol {

    private final Map<String, PNCounterImpl> counters;
    private final RequestRepository requestRepository;
    private View currentView;

    @Property
    private int numBackups = 2;

    private static ByteArray requestToBuffer(SizeStreamable req) throws IOException {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(req.serializedSize());
        req.writeTo(out);
        return new ByteArray(out.buffer(), 0, out.position());
    }

    public PN_COUNTER() {
        counters = new ConcurrentHashMap<>();
        requestRepository = new RequestRepository();
    }

    public int numBackups() {
        return numBackups;
    }

    public PN_COUNTER numBackups(int numBackups) {
        this.numBackups = numBackups;
        return this;
    }

    public PNCounter getOrCreateCounter(String name) {
        return internalGetOrCreate(name);
    }

    @Override
    public Object down(Event evt) {
        if (evt.getType() == Event.VIEW_CHANGE) {
            handleView(evt.arg());
        }
        return down_prot.down(evt);
    }

    @Override
    public Object up(Event evt) {
        if (evt.getType() == Event.VIEW_CHANGE) {
            handleView(evt.getArg());
        }
        return up_prot.up(evt);
    }

    @Override
    public Object up(Message msg) {
        PNCounterHeader header = msg.getHeader(getId());
        if (header == null) {
            return up_prot.up(msg);
        }
        handleMessage(header, msg);
        return null;
    }

    @Override
    public void up(MessageBatch batch) {
        for (Iterator<Message> iterator = batch.iterator(); iterator.hasNext(); ) {
            Message msg = iterator.next();
            PNCounterHeader header = msg.getHeader(getId());
            if (header != null) {
                handleMessage(header, msg);
                iterator.remove();
            }
        }
        if (batch.isEmpty()) {
            return;
        }
        up_prot.up(batch);
    }

    private PNCounterImpl internalGetOrCreate(String counterName) {
        return counters.computeIfAbsent(counterName, this::create);
    }

    private PNCounterImpl create(String name) {
        return new PNCounterImpl(name, local_addr, this);
    }

    private void handleView(View view) {
        // send the counter's state to new members
        List<Address> newMembers = Util.newElements(currentView.getMembers(), view.getMembers());
        this.currentView = view;
        if (newMembers.isEmpty()) {
            return;
        }
        for (Map.Entry<String, PNCounterImpl> entry : counters.entrySet()) {
            try {
                PNCounterSnapshot snapshot = entry.getValue().snapshot();
                ByteArray array = requestToBuffer(snapshot);
                PNCounterHeader header = PNCounterHeader.stateHeader(entry.getKey());
                for (Address dst : newMembers) {
                    BytesMessage msg = new BytesMessage(dst, array);
                    msg.putHeader(getId(), header);
                    down_prot.down(msg);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleMessage(PNCounterHeader header, Message msg) {
        PNCounterImpl counter = internalGetOrCreate(header.getCounterName());
        try {
            switch (header.getType()) {
                case PNCounterHeader.ACK:
                    requestRepository.ack(header.getReqId(), msg.src());
                    break;
                case PNCounterHeader.UPDATE:
                    assert msg.hasArray();
                    PNCounterStateSnapshot snapshot = new PNCounterStateSnapshot();
                    snapshot.readFrom(new ByteArrayDataInputStream(msg.getArray(), msg.getOffset(), msg.getLength()));
                    counter.onUpdate(msg.getSrc(), snapshot);
                    sendAck(header, msg);
                    break;
                case PNCounterHeader.STATE:
                    assert msg.hasArray();
                    PNCounterSnapshot cSnapshot = new PNCounterSnapshot(null);
                    cSnapshot.readFrom(new ByteArrayDataInputStream(msg.getArray(), msg.getOffset(), msg.getLength()));
                    counter.applySnapshot(cSnapshot);
                    break;
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendAck(PNCounterHeader header, Message msg) {
        Message ack = new EmptyMessage(msg.src());
        ack.putHeader(getId(), header.ack());
        down_prot.down(ack);
    }

    @Override
    public CompletionStage<Void> updateAllMembers(String counterName, PNCounterStateSnapshot snapshot) {
        try {
            ByteArray data = requestToBuffer(snapshot);
            BytesMessage msg = new BytesMessage(null, data);
            Request request = requestRepository.createRequest(currentView.getMembers(), numBackups);
            PNCounterHeader header = PNCounterHeader.updateHeader(request.getRequestId(), counterName);
            msg.putHeader(getId(), header);
            down_prot.down(msg);
            return request.toCompletionStage();
        } catch (IOException e) {
            return CompletableFuture.failedStage(e);
        }
    }
}

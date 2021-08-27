package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.blocks.atomic.PNCounter;
import org.jgroups.pncounter.PNCounterData;
import org.jgroups.pncounter.PNCounterHeader;
import org.jgroups.pncounter.PNCounterSnapshot;
import org.jgroups.pncounter.Request;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.ByteArray;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.jgroups.pncounter.RequestRepository.ACK;
import static org.jgroups.pncounter.RequestRepository.STATE;
import static org.jgroups.pncounter.RequestRepository.UPDATE;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 5.1
 */
@MBean(description = "Protocol to maintain positive-negative counters")
public class PN_COUNTER extends Protocol {

    private static final AtomicLong REQ_ID_GENERATOR = new AtomicLong();

    private final Map<String, CounterImpl> counters;
    private final Map<Long, Request> requestMap;
    private Address localAddress;
    private View currentView;

    private static PNCounterData createPNCounterData(Address ignored) {
        return new PNCounterData();
    }

    private static ByteArray requestToBuffer(byte reqType, SizeStreamable req) throws IOException {
        int size = req.serializedSize() + Global.BYTE_SIZE;
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(size);
        out.writeByte(reqType);
        req.writeTo(out);
        return new ByteArray(out.buffer(), 0, out.position());
    }

    public PN_COUNTER() {
        counters = new ConcurrentHashMap<>();
        requestMap = new ConcurrentHashMap<>();
    }

    public PNCounter getOrCreateCounter(String name) {
        return internalGetOrCreate(name);
    }

    @Override
    public Object down(Event evt) {
        switch (evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                localAddress = evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleView(evt.arg());
                break;
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
        try {
            handleMessage(header, msg);
        } catch (IOException | ClassNotFoundException e) {
            //TODO log
        }
        return null;

    }

    @Override
    public void up(MessageBatch batch) {
        for (Message message : batch.getMatchingMessages(getId(), true)) {
            try {
                handleMessage(message.getHeader(getId()), message);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        up_prot.up(batch);
    }

    private CounterImpl internalGetOrCreate(String counterName) {
        return counters.computeIfAbsent(counterName, this::create);
    }

    private CounterImpl create(String name) {
        return new CounterImpl(name, localAddress, this);
    }

    private void handleView(View view) {
        // send the counter's state to new members
        List<Address> oldView = currentView.getMembers();
        List<Address> newView = view.getMembers();
        this.currentView = view;
        List<Address> newMembers = Util.newElements(oldView, newView);
        if (newMembers.isEmpty()) {
            return;
        }
        for (Map.Entry<String, CounterImpl> entry : counters.entrySet()) {
            try {
                CounterSnapshot snapshot = entry.getValue().snapshot();
                ByteArray array = requestToBuffer(STATE, snapshot);
                PNCounterHeader header = new PNCounterHeader(-1, entry.getKey());
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

    private void handleMessage(PNCounterHeader header, Message msg) throws IOException, ClassNotFoundException {
        CounterImpl counter = internalGetOrCreate(header.getCounterName());
        DataInputStream is = new DataInputStream(new ByteArrayInputStream(msg.getArray(), msg.getOffset(), msg.getLength()));
        switch (is.readByte()) {
            case ACK:
                Request request = requestMap.get(header.getReqId());
                if (request == null) {
                    return;
                }
                request.onAck(msg.src());
            case UPDATE:
                PNCounterSnapshot snapshot = new PNCounterSnapshot();
                snapshot.readFrom(is);
                counter.onUpdate(msg.getSrc(), snapshot);
                sendAck(header, msg);
                break;
            case STATE:
                CounterSnapshot cSnapshot = new CounterSnapshot(null);
                cSnapshot.readFrom(is);
                counter.applySnapshot(cSnapshot);
                break;
        }
    }

    private void sendAck(PNCounterHeader header, Message msg) {
        BytesMessage ack = new BytesMessage(msg.src());
        PNCounterHeader ackHeader = header.ack();
        ack.putHeader(getId(), ackHeader);
        ack.setArray(new byte[]{ACK});
        down_prot.down(ack);
    }

    private CompletionStage<Void> updateAllMembers(String counterName, PNCounterSnapshot snapshot) {
        try {
            ByteArray data = requestToBuffer(UPDATE, snapshot);
            BytesMessage msg = new BytesMessage(null, data);
            PNCounterHeader header = new PNCounterHeader(REQ_ID_GENERATOR.incrementAndGet(), counterName);
            msg.putHeader(getId(), header);
            Request request = new Request();
            requestMap.put(header.getReqId(), request);
            down_prot.down(msg);
            return request.toCompletionStage();
        } catch (IOException e) {
            return CompletableFuture.failedStage(e);
        }
    }

    private static class CounterImpl implements PNCounter {

        private final String name;
        private final PN_COUNTER protocol;
        private final Address local;
        private final Map<Address, PNCounterData> counter;

        private CounterImpl(String name, Address localAddress, PN_COUNTER protocol) {
            this.name = name;
            this.protocol = protocol;
            this.local = localAddress;
            this.counter = new ConcurrentHashMap<>();
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public long get() {
            return counter.values().stream().map(PNCounterData::sum).reduce(0L, Long::sum);
        }

        @Override
        public CompletionStage<Void> add(long value) {
            if (value == 0) {
                return null;
            }
            PNCounterData data = counter.computeIfAbsent(local, PN_COUNTER::createPNCounterData);
            PNCounterSnapshot snapshot = data.add(value);
            return protocol.updateAllMembers(name, snapshot);
        }

        void onUpdate(Address originator, PNCounterSnapshot snapshot) {
            PNCounterData data = counter.computeIfAbsent(originator, PN_COUNTER::createPNCounterData);
            data.update(snapshot);
        }

        CounterSnapshot snapshot() {
            Map<Address, PNCounterSnapshot> snapshotMap = new HashMap<>();
            for (Map.Entry<Address, PNCounterData> entry : counter.entrySet()) {
                snapshotMap.put(entry.getKey(), entry.getValue().snapshot());
            }
            return new CounterSnapshot(snapshotMap);
        }

        void applySnapshot(CounterSnapshot snapshot) {
            for (Map.Entry<Address, PNCounterSnapshot> entry : snapshot.getCounter().entrySet()) {
                counter.computeIfAbsent(entry.getKey(), PN_COUNTER::createPNCounterData).update(entry.getValue());
            }
        }
    }

    private static class CounterSnapshot implements SizeStreamable {

        private Map<Address, PNCounterSnapshot> counter;

        public CounterSnapshot(Map<Address, PNCounterSnapshot> counter) {
            this.counter = counter;
        }

        public Map<Address, PNCounterSnapshot> getCounter() {
            return counter;
        }

        @Override
        public int serializedSize() {
            int numberOfEntries = counter.size();
            int size = counter.entrySet().stream().map(entry -> Util.size(entry.getKey()) + entry.getValue().serializedSize()).reduce(0, Integer::sum);
            return Bits.size(numberOfEntries) + size;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeIntCompressed(counter.size(), out);
            for (Map.Entry<Address, PNCounterSnapshot> entry : counter.entrySet()) {
                Util.writeAddress(entry.getKey(), out);
                entry.getValue().writeTo(out);
            }
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            int numberOfEntries = Bits.readIntCompressed(in);
            this.counter = new HashMap<>();
            for (int i = 0; i < numberOfEntries; ++i) {
                Address address = Util.readAddress(in);
                PNCounterSnapshot snapshot = new PNCounterSnapshot();
                snapshot.readFrom(in);

                counter.put(address, snapshot);
            }
        }
    }
}

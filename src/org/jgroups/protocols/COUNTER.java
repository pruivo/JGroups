package org.jgroups.protocols;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.Buffer;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.CompletableFutures;
import org.jgroups.util.Owner;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Streamable;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;


/**
 * Protocol which is used by {@link org.jgroups.blocks.atomic.CounterService} to provide a distributed atomic counter
 * @author Bela Ban
 * @since 3.0.0
 */
@MBean(description="Protocol to maintain distributed atomic counters")
public class COUNTER extends Protocol {

    private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong();
    //enum value() method is expensive since it creates a copy of the internal array every time it is invoked.
    //we can cache it since we don't change it.
    private static final RequestType[] REQUEST_TYPES_CACHED = RequestType.values();
    private static final ResponseType[] RESPONSE_TYPES_CACHED = ResponseType.values();

    @Property(description="Bypasses message bundling if true")
    protected boolean bypass_bundling=true;

    @Property(description="Request timeouts (in ms). If the timeout elapses, a Timeout (runtime) exception will be thrown")
    protected long timeout=60000;

    @Property(description="Number of milliseconds to wait for reconciliation responses from all current members")
    protected long reconciliation_timeout=10000;

    @Property(description="Number of backup coordinators. Modifications are asynchronously sent to all backup coordinators")
    protected int num_backups=1;

    protected Address local_addr;

    /** Set to true during reconciliation process, will cause all requests to be discarded */
    protected boolean discard_requests=false;

    protected View    view;

    /** The address of the cluster coordinator. Updated on view changes */
    protected Address coord;

    /** Backup coordinators. Only created if num_backups > 0 and coord=true */
    protected List<Address> backup_coords=null;

    protected Future<?> reconciliation_task_future;

    protected ReconciliationTask reconciliation_task;

    // server side counters
    protected final Map<String, VersionedValue> counters = Util.createConcurrentMap(20);

    // (client side) pending requests
    protected final Map<Owner, Tuple<Request, CompletableFuture<ResponseData>>> pending_requests = Util.createConcurrentMap(20);

    protected static final byte REQUEST  = 1;
    protected static final byte RESPONSE = 2;


    protected enum RequestType {
        GET_OR_CREATE {
            @Override
            Request create() {
                return new GetOrCreateRequest();
            }
        },
        DELETE {
            @Override
            Request create() {
                return new DeleteRequest();
            }
        },
        SET {
            @Override
            Request create() {
                return new SetRequest();
            }
        },
        COMPARE_AND_SET {
            @Override
            Request create() {
                return new CompareAndSetRequest();
            }
        },
        ADD_AND_GET {
            @Override
            Request create() {
                return new AddAndGetRequest();
            }
        },
        UPDATE {
            @Override
            Request create() {
                return new UpdateRequest();
            }
        },
        RECONCILE {
            @Override
            Request create() {
                return new ReconcileRequest();
            }
        },
        RESEND_PENDING_REQUESTS {
            @Override
            Request create() {
                return new ResendPendingRequests();
            }
        };

        abstract Request create();
    }

    protected enum ResponseType {
        VALUE {
            @Override
            Response create() {
                return new ValueResponse();
            }
        },
        EXCEPTION{
            @Override
            Response create() {
                return new ExceptionResponse();
            }
        },
        RECONCILE {
            @Override
            Response create() {
                return new ReconcileResponse();
            }
        };

        abstract Response create();
    }

    public boolean getBypassBundling() {
        return bypass_bundling;
    }

    public void setBypassBundling(boolean bypass_bundling) {
        this.bypass_bundling=bypass_bundling;
    }


    @ManagedAttribute
    public String getAddress() {
        return local_addr != null? local_addr.toString() : null;
    }

    @ManagedAttribute
    public String getView() {
        return view != null? view.toString() : null;
    }

    @ManagedAttribute(description="List of the backup coordinator (null if num_backups <= 0")
    public String getBackupCoords() {
        return backup_coords != null? backup_coords.toString() : "null";
    }


    public Counter getOrCreateCounter(String name, long initial_value) {
        if(local_addr == null)
            throw new IllegalArgumentException("the channel needs to be connected before creating or getting a counter");
        Owner owner=getOwner();
        GetOrCreateRequest req=new GetOrCreateRequest(owner, name, initial_value);
        CompletableFuture<ResponseData> cf = new CompletableFuture<>();
        pending_requests.put(owner, new Tuple<>(req, cf));
        sendRequest(coord, req);
        try {
            updateCounter(cf.join());
            return new CounterImpl(name);
        } catch(CompletionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /** Sent asynchronously - we don't wait for an ack */
    public void deleteCounter(String name) {
        Owner owner=getOwner();
        Request req=new DeleteRequest(owner, name);
        sendRequest(coord, req);
        if(!local_addr.equals(coord))
            counters.remove(name);
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleView(evt.arg());
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        if (evt.getType() == Event.VIEW_CHANGE) {
            handleView(evt.getArg());
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        CounterHeader hdr=msg.getHeader(id);
        if(hdr == null)
            return up_prot.up(msg);

        try {
            Object obj=streamableFromBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            if(log.isTraceEnabled())
                log.trace("[" + local_addr + "] <-- [" + msg.getSrc() + "] " + obj);

            if(obj instanceof Request) {
                handleRequest((Request)obj, msg.getSrc());
            }
            else if(obj instanceof Response) {
                handleResponse((Response)obj, msg.getSrc());
            }
            else {
                log.error(Util.getMessage("ReceivedObjectIsNeitherARequestNorAResponse") + obj);
            }
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedHandlingMessage"), ex);
        }
        return null;
    }


    protected void handleRequest(Request req, Address sender) {
        req.execute(this, sender);
    }


    protected VersionedValue getCounter(String name) {
        VersionedValue val=counters.get(name);
        if(val == null)
            throw new IllegalStateException("counter \"" + name + "\" not found");
        return val;
    }

    protected void handleResponse(Response rsp, Address sender) {
        if(rsp instanceof ReconcileResponse) {
            handleReconcileResponse((ReconcileResponse) rsp, sender);
            return;
        }

        Tuple<Request,CompletableFuture<ResponseData>> tuple=pending_requests.remove(rsp.getOwner());
        if(tuple == null) {
            log.warn("response for " + rsp.getOwner() + " didn't have an entry");
            return;
        }
        rsp.complete(tuple.getVal1().getCounterName(), tuple.getVal2());
    }

    private void handleReconcileResponse(ReconcileResponse rsp, Address sender) {
        if(log.isTraceEnabled() && rsp.names != null && rsp.names.length > 0)
            log.trace("[" + local_addr + "] <-- [" + sender + "] RECONCILE-RSP: " + dump(rsp.names, rsp.values, rsp.versions));
        if(reconciliation_task != null)
            reconciliation_task.add(rsp, sender);

    }

    @ManagedOperation(description="Dumps all counters")
    public String printCounters() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,VersionedValue> entry: counters.entrySet())
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        return sb.toString();
    }

    @ManagedOperation(description="Dumps all pending requests")
    public String dumpPendingRequests() {
        StringBuilder sb=new StringBuilder();
        for(Tuple<Request,CompletableFuture<ResponseData>> tuple: pending_requests.values()) {
            Request tmp=tuple.getVal1();
            sb.append(tmp).append('(').append(tmp.getClass().getCanonicalName()).append(") ");
        }
        return sb.toString();
    }

    protected void handleView(View view) {
        this.view=view;
        if(log.isDebugEnabled())
            log.debug("view=" + view);
        List<Address> members=view.getMembers();
        Address old_coord=coord;
        if(!members.isEmpty())
            coord=members.get(0);

        if(Objects.equals(coord, local_addr)) {
            List<Address> old_backups=backup_coords != null? new ArrayList<>(backup_coords) : null;
            backup_coords=new CopyOnWriteArrayList<>(Util.pickNext(members, local_addr, num_backups));

            // send the current values to all *new* backups
            List<Address> new_backups=Util.newElements(old_backups,backup_coords);
            for(Address new_backup: new_backups) {
                for(Map.Entry<String,VersionedValue> entry: counters.entrySet()) {
                    UpdateRequest update=new UpdateRequest(entry.getKey(), entry.getValue().value, entry.getValue().version);
                    sendRequest(new_backup, update);
                }
            }
        }
        else
            backup_coords=null;

        if(old_coord != null && coord != null && !old_coord.equals(coord) && local_addr.equals(coord)) {
            discard_requests=true; // set to false when the task is done
            startReconciliationTask();
        }
    }


    protected Owner getOwner() {
        return new Owner(local_addr, REQUEST_ID_GENERATOR.incrementAndGet());
    }

    protected void updateBackups(String name, long[] versionedValue) {
        if (backup_coords == null || backup_coords.isEmpty()) {
            return;
        }
        Request req=new UpdateRequest(name, versionedValue[0], versionedValue[1]);
        try {
            Buffer buffer=requestToBuffer(req);
            for(Address backup_coord: backup_coords)
                send(backup_coord, buffer);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + req + " to backup coordinator(s):" + ex);
        }
    }

    protected void sendRequest(Address dest, Request req) {
        try {
            Buffer buffer=requestToBuffer(req);
            logSending(dest, req);
            send(dest, buffer);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + req + " request: " + ex);
        }
    }


    protected void sendResponse(Address dest, Response rsp) {
        try {
            Buffer buffer=responseToBuffer(rsp);
            logSending(dest, rsp);
            send(dest, buffer);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSending") + rsp + " message to " + dest + ": " + ex);
        }
    }

    protected void send(Address dest, Buffer buffer) {
        try {
            Message rsp_msg=new Message(dest, buffer).putHeader(id, new CounterHeader());
            if(bypass_bundling)
                rsp_msg.setFlag(Message.Flag.DONT_BUNDLE);
            down_prot.down(rsp_msg);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedSendingMessageTo") + dest + ": " + ex);
        }
    }

    private void logSending(Address dst, Object data) {
        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "] --> [" + (dst == null? "ALL" : dst) + "]: " + data);
    }

    protected void sendCounterNotFoundExceptionResponse(Address dest, Owner owner, String counter_name) {
        Response rsp=new ExceptionResponse(owner, "counter \"" + counter_name + "\" not found");
        sendResponse(dest, rsp);
    }

    private long updateCounter(ResponseData responseData) {
        if(!coord.equals(local_addr)) {
            counters.compute(responseData.counterName, responseData);
        }
        return responseData.value;
    }


    protected static Buffer requestToBuffer(Request req) throws Exception {
        return streamableToBuffer(REQUEST,(byte)req.getRequestType().ordinal(), req);
    }

    protected static Buffer responseToBuffer(Response rsp) throws Exception {
        return streamableToBuffer(RESPONSE,(byte)rsp.getResponseType().ordinal(), rsp);
    }

    protected static Buffer streamableToBuffer(byte req_or_rsp, byte type, Streamable obj) throws Exception {
        int expected_size=obj instanceof SizeStreamable? ((SizeStreamable)obj).serializedSize() : 100;
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size);
        out.writeByte(req_or_rsp);
        out.writeByte(type);
        obj.writeTo(out);
        return new Buffer(out.buffer(), 0, out.position());
    }

    protected static Streamable streamableFromBuffer(byte[] buf, int offset, int length) throws Exception {
        switch(buf[offset]) {
            case REQUEST:
                return requestFromBuffer(buf, offset+1, length-1);
            case RESPONSE:
                return responseFromBuffer(buf, offset+1, length-1);
            default:
                throw new IllegalArgumentException("type " + buf[offset] + " is invalid (expected Request (1) or RESPONSE (2)");
        }
    }

    protected static Request requestFromBuffer(byte[] buf, int offset, int length) throws Exception {
        ByteArrayInputStream input=new ByteArrayInputStream(buf, offset, length);
        DataInputStream in=new DataInputStream(input);
        Request retval=REQUEST_TYPES_CACHED[in.readByte()].create();
        retval.readFrom(in);
        return retval;
    }

    protected static Response responseFromBuffer(byte[] buf, int offset, int length) throws Exception {
        ByteArrayInputStream input=new ByteArrayInputStream(buf, offset, length);
        DataInputStream in=new DataInputStream(input);
        Response retval=RESPONSE_TYPES_CACHED[in.readByte()].create();
        retval.readFrom(in);
        return retval;
    }


    protected synchronized void startReconciliationTask() {
        if(reconciliation_task_future == null || reconciliation_task_future.isDone()) {
            reconciliation_task=new ReconciliationTask();
            reconciliation_task_future=getTransport().getTimer().schedule(reconciliation_task, 0, TimeUnit.MILLISECONDS);
        }
    }

    //TODO! not used? can be removed?
//    protected synchronized void stopReconciliationTask() {
//        if(reconciliation_task_future != null) {
//            reconciliation_task_future.cancel(true);
//            if(reconciliation_task != null)
//                reconciliation_task.cancel();
//            reconciliation_task_future=null;
//        }
//    }


    protected static void writeReconciliation(DataOutput out, String[] names, long[] values, long[] versions) throws IOException {
        if(names == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(names.length);
        for(String name: names)
            Bits.writeString(name,out);
        for(long value: values)
            Bits.writeLong(value, out);
        for(long version: versions)
            Bits.writeLong(version, out);
    }

    protected static String[] readReconciliationNames(DataInput in, int len) throws IOException {
        String[] retval=new String[len];
        for(int i=0; i < len; i++)
            retval[i]=Bits.readString(in);
        return retval;
    }

    protected static long[] readReconciliationLongs(DataInput in, int len) throws IOException {
        long[] retval=new long[len];
        for(int i=0; i < len; i++)
            retval[i]=Bits.readLong(in);
        return retval;
    }

    protected static String dump(String[] names, long[] values, long[] versions) {
        StringBuilder sb=new StringBuilder();
        if(names != null) {
            for(int i=0; i < names.length; i++) {
                sb.append(names[i]).append(": ").append(values[i]).append(" (").append(versions[i]).append(")\n");
            }
        }
        return sb.toString();
    }


    protected class CounterImpl implements Counter {
        protected final String  name;

        protected CounterImpl(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public CompletableFuture<Long> getAsync() {
            return addAndGetAsync(0);
        }

        @Override
        public CompletableFuture<Void> setAsync(long new_value) {
            if(local_addr.equals(coord)) {
                VersionedValue val=getCounter(name);
                long[] result = val.set(new_value);
                updateBackups(name, result);
                return CompletableFutures.completedNull();
            }
            Owner owner=getOwner();
            Request req=new SetRequest(owner, name, new_value);
            return sendRequestToCoordinator(owner, req).thenAccept(CompletableFutures.voidConsumer());
        }

        @Override
        public CompletableFuture<Long> compareAndSwapAsync(long expect, long update) {
            if(local_addr.equals(coord)) {
                VersionedValue val=getCounter(name);
                long retval=val.compareAndSwap(expect, update)[0];
                updateBackups(name, val.snapshot());
                return CompletableFuture.completedFuture(retval);
            }
            Owner owner=getOwner();
            Request req=new CompareAndSetRequest(owner, name, expect, update);
            return sendRequestToCoordinator(owner, req);
        }

        @Override
        public CompletableFuture<Long> addAndGetAsync(long delta) {
            if(local_addr.equals(coord)) {
                VersionedValue val=getCounter(name);
                long[] result=val.addAndGet(delta);
                updateBackups(name, result);
                return CompletableFuture.completedFuture(result[0]);
            }
            Owner owner=getOwner();
            Request req=new AddAndGetRequest(owner, name, delta);
            return sendRequestToCoordinator(owner, req);
        }

        @Override
        public String toString() {
            VersionedValue val=counters.get(name);
            return val != null? val.toString() : "n/a";
        }
    }

    private CompletableFuture<Long> sendRequestToCoordinator(Owner owner, Request request) {
        CompletableFuture<ResponseData> cf = new CompletableFuture<>();
        pending_requests.put(owner, new Tuple<>(request, cf));
        sendRequest(coord, request);
        TimeBasedCompletableFuture<ResponseData> tcf = new TimeBasedCompletableFuture<>(cf);
        TP tp = getTransport();
        tcf.scheduleTimeout(tp.getTimer(), timeout);
        return cf.thenApplyAsync(this::updateCounter, tp.getThreadPool());
    }

    private static class TimeBasedCompletableFuture<T> implements Runnable, Consumer<T> {

        private final CompletableFuture<T> completableFuture;
        private volatile Future<?> timeoutFuture;

        private TimeBasedCompletableFuture(CompletableFuture<T> completableFuture) {
            this.completableFuture = Objects.requireNonNull(completableFuture);
            this.timeoutFuture = CompletableFutures.nullFuture();
        }

        @Override
        public void run() {
            //on timeout, do:
            completableFuture.completeExceptionally(new TimeoutException());
        }

        @Override
        public void accept(T t) {
            //when completable future successful completed, do:
            timeoutFuture.cancel(true);
        }

        void scheduleTimeout(TimeScheduler scheduler, long timeout) {
            timeoutFuture = scheduler.schedule(this, timeout, TimeUnit.MILLISECONDS);
            completableFuture.thenAccept(this);
        }
    }

    private boolean skipRequest() {
        return !local_addr.equals(coord) || discard_requests;
    }

    protected interface Request extends Streamable {

        String getCounterName();

        RequestType getRequestType();

        void execute(COUNTER protocol, Address sender);
    }


    protected abstract static class SimpleRequest implements Request {
        protected Owner   owner;
        protected String  name;


        protected SimpleRequest() {
        }

        protected SimpleRequest(Owner owner, String name) {
            this.owner=owner;
            this.name=name;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            owner.writeTo(out);
            Bits.writeString(name,out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            owner=new Owner();
            owner.readFrom(in);
            name=Bits.readString(in);
        }

        public String toString() {
            return owner + " [" + name + "]";
        }

        @Override
        public String getCounterName() {
            return name;
        }
    }

    protected static class ResendPendingRequests implements Request {
        @Override
        public void writeTo(DataOutput out) throws IOException {}
        @Override
        public void readFrom(DataInput in) throws IOException {}
        public String toString() {return "ResendPendingRequests";}

        @Override
        public String getCounterName() {
            return null;
        }

        @Override
        public RequestType getRequestType() {
            return RequestType.RESEND_PENDING_REQUESTS;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            for(Tuple<Request,CompletableFuture<ResponseData>> tuple: protocol.pending_requests.values()) {
                Request request=tuple.getVal1();
                protocol.traceResending(request);
                protocol.sendRequest(protocol.coord, request);
            }
        }
    }

    private void traceResending(Request request) {
        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "] --> [" + coord + "] resending " + request);
    }

    protected static class GetOrCreateRequest extends SimpleRequest {
        protected long initial_value;

        protected GetOrCreateRequest() {}

        GetOrCreateRequest(Owner owner, String name, long initial_value) {
            super(owner,name);
            this.initial_value=initial_value;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            initial_value=Bits.readLong(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeLong(initial_value, out);
        }

        @Override
        public RequestType getRequestType() {
            return RequestType.GET_OR_CREATE;
        }

        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            VersionedValue new_val=new VersionedValue(initial_value);
            VersionedValue val=protocol.counters.putIfAbsent(name, new_val);
            if(val == null)
                val=new_val;
            long[] result = val.snapshot();
            Response rsp=new ValueResponse(owner, result);
            protocol.sendResponse(sender,rsp);
            protocol.updateBackups(name, result);
        }
    }


    protected static class DeleteRequest extends SimpleRequest {

        protected DeleteRequest() {}

        protected DeleteRequest(Owner owner, String name) {
            super(owner,name);
        }

        public String toString() {return "DeleteRequest: " + super.toString();}

        @Override
        public RequestType getRequestType() {
            return RequestType.DELETE;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            protocol.counters.remove(name);
        }
    }


    protected static class AddAndGetRequest extends SetRequest {
        protected AddAndGetRequest() {}

        protected AddAndGetRequest(Owner owner, String name, long value) {
            super(owner,name,value);
        }

        public String toString() {return "AddAndGetRequest: " + super.toString();}

        @Override
        public RequestType getRequestType() {
            return RequestType.ADD_AND_GET;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            VersionedValue val=protocol.counters.get(name);
            if(val == null) {
                protocol.sendCounterNotFoundExceptionResponse(sender, owner, name);
                return;
            }
            long[] result=val.addAndGet(value);
            Response rsp=new ValueResponse(owner, result);
            protocol.sendResponse(sender, rsp);
            protocol.updateBackups(name, result);
        }
    }



    protected static class SetRequest extends SimpleRequest {
        protected long value;

        protected SetRequest() {}

        protected SetRequest(Owner owner, String name, long value) {
            super(owner, name);
            this.value=value;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            value=Bits.readLong(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeLong(value, out);
        }

        public String toString() {return super.toString() + ": " + value;}

        @Override
        public RequestType getRequestType() {
            return RequestType.SET;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            VersionedValue val=protocol.counters.get(name);
            if(val == null) {
                protocol.sendCounterNotFoundExceptionResponse(sender, owner, name);
                return;
            }
            long[] result=val.set(value);
            Response rsp=new ValueResponse(owner, result);
            protocol.sendResponse(sender, rsp);
            protocol.updateBackups(name, result);
        }
    }


    protected static class CompareAndSetRequest extends SimpleRequest {
        protected long expected, update;

        protected CompareAndSetRequest() {}

        protected CompareAndSetRequest(Owner owner, String name, long expected, long update) {
            super(owner, name);
            this.expected=expected;
            this.update=update;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            expected=Bits.readLong(in);
            update=Bits.readLong(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeLong(expected, out);
            Bits.writeLong(update, out);
        }

        public String toString() {return super.toString() + ", expected=" + expected + ", update=" + update;}

        @Override
        public RequestType getRequestType() {
            return RequestType.COMPARE_AND_SET;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(protocol.skipRequest())
                return;
            VersionedValue val=protocol.counters.get(name);
            if(val == null) {
                protocol.sendCounterNotFoundExceptionResponse(sender, owner, name);
                return;
            }
            long[] result=val.compareAndSwap(expected, update);
            Response rsp=new ValueResponse(owner, result);
            protocol.sendResponse(sender, rsp);
            protocol.updateBackups(name, val.snapshot());
        }
    }


    protected static class ReconcileRequest implements Request {
        protected String[] names;
        protected long[]   values;
        protected long[]   versions;

        protected ReconcileRequest() {}

        protected ReconcileRequest(String[] names, long[] values, long[] versions) {
            this.names=names;
            this.values=values;
            this.versions=versions;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            writeReconciliation(out, names, values, versions);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            int len=in.readInt();
            names=readReconciliationNames(in, len);
            values=readReconciliationLongs(in, len);
            versions=readReconciliationLongs(in,len);
        }

        public String toString() {return "ReconcileRequest (" + names.length + ") entries";}

        @Override
        public String getCounterName() {
            return null;
        }

        @Override
        public RequestType getRequestType() {
            return RequestType.RECONCILE;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            if(sender.equals(protocol.local_addr)) // we don't need to reply to our own reconciliation request
                return;

            // return all values except those with lower or same versions than the ones in the ReconcileRequest
            Map<String,VersionedValue> map=new HashMap<>(protocol.counters);
            if(names !=  null) {
                for(int i=0; i < names.length; i++) {
                    String counter_name=names[i];
                    long version=versions[i];
                    VersionedValue my_value=map.get(counter_name);
                    if(my_value != null && my_value.version <= version)
                        map.remove(counter_name);
                }
            }

            int len=map.size();
            String[] names=new String[len];
            long[] values=new long[len];
            long[] versions=new long[len];
            int index=0;
            for(Map.Entry<String,VersionedValue> entry: map.entrySet()) {
                names[index]=entry.getKey();
                values[index]=entry.getValue().value;
                versions[index]=entry.getValue().version;
                index++;
            }

            Response rsp=new ReconcileResponse(names, values, versions);
            protocol.sendResponse(sender, rsp);
        }
    }


    protected static class UpdateRequest implements Request, BiFunction<String, VersionedValue, VersionedValue> {
        protected String name;
        protected long   value;
        protected long   version;

        protected UpdateRequest() {}

        protected UpdateRequest(String name, long value, long version) {
            this.name=name;
            this.value=value;
            this.version=version;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeString(name,out);
            Bits.writeLong(value, out);
            Bits.writeLong(version, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            name=Bits.readString(in);
            value=Bits.readLong(in);
            version=Bits.readLong(in);
        }

        public String toString() {return "UpdateRequest(" + name + ": "+ value + " (" + version + ")";}

        @Override
        public String getCounterName() {
            return name;
        }

        @Override
        public RequestType getRequestType() {
            return RequestType.UPDATE;
        }

        @Override
        public void execute(COUNTER protocol, Address sender) {
            protocol.counters.compute(name, this);
        }

        @Override
        public VersionedValue apply(String name, VersionedValue versionedValue) {
            if (versionedValue == null) {
                versionedValue = new VersionedValue(value, version);
            } else {
                versionedValue.updateIfBigger(value, version);
            }
            return versionedValue;
        }
    }

    private static abstract class Response implements Streamable {

        private Owner owner;

        Response() {}

        Response(Owner owner) {
            this.owner = owner;
        }

        abstract ResponseType getResponseType();

        abstract void complete(String counterName, CompletableFuture<ResponseData> cf);

        final Owner getOwner() {
            return owner;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            owner.writeTo(out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            owner = new Owner();
            owner.readFrom(in);
        }
    }


    protected static class ValueResponse extends Response {
        protected long result;
        protected long version;

        protected ValueResponse() {}

        ValueResponse(Owner owner, long[] versionedValue) {
            this(owner, versionedValue[0], versionedValue[1]);
        }

        protected ValueResponse(Owner owner, long result, long version) {
            super(owner);
            this.result=result;
            this.version=version;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            result=Bits.readLong(in);
            version=Bits.readLong(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeLong(result, out);
            Bits.writeLong(version, out);
        }

        public String toString() {return "ValueResponse(" + result + ")";}

        @Override
        public ResponseType getResponseType() {
            return ResponseType.VALUE;
        }

        @Override
        void complete(String counterName, CompletableFuture<ResponseData> cf) {
            cf.complete(new ResponseData(counterName, result, version));
        }
    }


    protected static class ExceptionResponse extends Response {
        protected String error_message;

        protected ExceptionResponse() {}

        protected ExceptionResponse(Owner owner, String error_message) {
            super(owner);
            this.error_message=error_message;
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            super.readFrom(in);
            error_message=Bits.readString(in);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeString(error_message,out);
        }

        public String toString() {return "ExceptionResponse: " + super.toString();}

        @Override
        public ResponseType getResponseType() {
            return ResponseType.EXCEPTION;
        }

        @Override
        void complete(String counterName, CompletableFuture<ResponseData> cf) {
            cf.completeExceptionally(new Throwable(error_message));
        }
    }

    protected static class ReconcileResponse extends Response {
        protected String[] names;
        protected long[]   values;
        protected long[]   versions;

        protected ReconcileResponse() {}

        protected ReconcileResponse(String[] names, long[] values, long[] versions) {
            this.names=names;
            this.values=values;
            this.versions=versions;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            writeReconciliation(out,names,values,versions);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            int len=in.readInt();
            names=readReconciliationNames(in, len);
            values=readReconciliationLongs(in, len);
            versions=readReconciliationLongs(in,len);
        }

        public String toString() {
            int num=names != null? names.length : 0;
            return "ReconcileResponse (" + num + ") entries";
        }

        @Override
        public ResponseType getResponseType() {
            return ResponseType.RECONCILE;
        }

        @Override
        void complete(String counterName, CompletableFuture<ResponseData> cf) {
            //no-op
        }
    }
    


    public static class CounterHeader extends Header {
        public Supplier<? extends Header> create() {return CounterHeader::new;}
        public short getMagicId() {return 74;}
        @Override
        public int serializedSize() {return 0;}
        @Override
        public void writeTo(DataOutput out) {}
        @Override
        public void readFrom(DataInput in) {}
    }
    

    protected static class VersionedValue {
        protected long value;
        protected long version=1;

        protected VersionedValue(long value) {
            this.value=value;
        }

        protected VersionedValue(long value, long version) {
            this.value=value;
            this.version=version;
        }

        /** num == 0 --> GET */
        protected synchronized long[] addAndGet(long num) {
            return num == 0? new long[]{value, version} : new long[]{value+=num, ++version};
        }

        protected synchronized long[] set(long value) {
            return new long[]{this.value=value,++version};
        }

        protected synchronized long[] compareAndSwap(long expected, long update) {
            long oldValue = value;
            if(oldValue == expected) {
                value = update;
                ++version;
            }
            return new long[]{oldValue, version};
        }

        /** Sets the value only if the version argument is greater than the own version */
        protected synchronized void updateIfBigger(long value, long version) {
            if(version > this.version) {
                this.version=version;
                this.value=value;
            }
        }

        synchronized long[] snapshot() {
            return new long[] {value, version};
        }

        public String toString() {return value + " (version=" + version + ")";}
    }


    protected class ReconciliationTask implements Runnable {
        protected ResponseCollector<ReconcileResponse> responses;


        public void run() {
            try {
                _run();
            }
            finally {
                discard_requests=false;
            }

            Request req=new ResendPendingRequests();
            sendRequest(null, req);
        }


        protected void _run() {
            Map<String,VersionedValue> copy=new HashMap<>(counters);
            int len=copy.size();
            String[] names=new String[len];
            long[] values=new long[len], versions=new long[len];
            int index=0;
            for(Map.Entry<String,VersionedValue> entry: copy.entrySet()) {
                names[index]=entry.getKey();
                values[index]=entry.getValue().value;
                versions[index]=entry.getValue().version;
                index++;
            }
            List<Address> targets=new ArrayList<>(view.getMembers());
            targets.remove(local_addr);
            responses=new ResponseCollector<>(targets); // send to everyone but us
            Request req=new ReconcileRequest(names, values, versions);
            sendRequest(null, req);

            responses.waitForAllResponses(reconciliation_timeout);
            Map<Address,ReconcileResponse> reconcile_results=responses.getResults();
            for(Map.Entry<Address,ReconcileResponse> entry: reconcile_results.entrySet()) {
                if(entry.getKey().equals(local_addr))
                    continue;
                ReconcileResponse rsp=entry.getValue();
                if(rsp != null && rsp.names != null) {
                    for(int i=0; i < rsp.names.length; i++) {
                        String counter_name=rsp.names[i];
                        long version=rsp.versions[i];
                        long value=rsp.values[i];
                        VersionedValue my_value=counters.get(counter_name);
                        if(my_value == null) {
                            counters.put(counter_name, new VersionedValue(value, version));
                            continue;
                        }

                        if(my_value.version < version)
                            my_value.updateIfBigger(value, version);
                    }
                }
            }
        }


        public void add(ReconcileResponse rsp, Address sender) {
            if(responses != null)
                responses.add(sender, rsp);
        }

        protected void cancel() {
            if(responses != null)
                responses.reset();
        }

        public String toString() {
            return COUNTER.class.getSimpleName() + ": " + getClass().getSimpleName();
        }
    }

    private static class ResponseData implements BiFunction<String, VersionedValue, VersionedValue> {
        private final String counterName;
        private final long value;
        private final long version;

        private ResponseData(String counterName, long value, long version) {
            this.counterName = counterName;
            this.value = value;
            this.version = version;
        }

        /**
         * Updates the VersionedValue if the version is bigger.
         */
        @Override
        public VersionedValue apply(String s, VersionedValue versionedValue) {
            if (versionedValue == null) {
                versionedValue = new VersionedValue(value, version);
            } else {
                versionedValue.updateIfBigger(value, version);
            }
            return versionedValue;
        }
    }
}

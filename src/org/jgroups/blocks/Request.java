package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Abstract class for a unicast or multicast request
 *
 * @author Bela Ban
 */
public abstract class Request implements RspCollector, NotifyingFuture {
    protected static final Log        log=LogFactory.getLog(Request.class);

    /** To generate unique request IDs (see getRequestId()) */
    protected static final AtomicLong REQUEST_ID=new AtomicLong(1);

    protected final Lock              lock=new ReentrantLock();

    /** Is set as soon as the request has received all required responses */
    protected final Condition         completed=lock.newCondition();

    protected final  Message          request_msg;
    protected final RequestCorrelator corr;         // either use RequestCorrelator or ...

    protected final RequestOptions    options;

    protected volatile boolean        done;
    protected boolean                 block_for_results=true;
    protected final long              req_id; // request ID for this request

    protected volatile FutureListener listener;


    
    public Request(Message request, RequestCorrelator corr, RequestOptions options) {
        this.request_msg=request;
        this.corr=corr;
        this.options=options;
        this.req_id=getRequestId();
    }


    public void setResponseFilter(RspFilter filter) {
        options.setRspFilter(filter);
    }

    public boolean getBlockForResults() {
        return block_for_results;
    }

    public void setBlockForResults(boolean block_for_results) {
        this.block_for_results=block_for_results;
    }

    public NotifyingFuture setListener(FutureListener listener) {
        this.listener=listener;
        if(done)
            listener.futureDone(this);
        return this;
    }

    public boolean execute() throws Exception {
        if(corr == null) {
            if(log.isErrorEnabled()) log.error("corr is null, cannot send request");
            return false;
        }

        sendRequest();
        if(!block_for_results || options.getMode() == ResponseMode.GET_NONE)
            return true;

        lock.lock();
        try {
            return responsesComplete(options.getTimeout());
        }
        finally {
            done=true;
            lock.unlock();
        }
    }

    protected abstract void sendRequest() throws Exception;

    public abstract void receiveResponse(Object response_value, Address sender, boolean is_exception);

    public abstract void viewChange(View new_view);

    public abstract void suspect(Address mbr);

    protected abstract boolean responsesComplete();


    public boolean getResponsesComplete() {
        lock.lock();
        try {
            return responsesComplete();
        }
        finally {
            lock.unlock();
        }
    }


    public boolean cancel(boolean mayInterruptIfRunning) {
        lock.lock();
        try {
            boolean retval=!done;
            done=true;
            if(corr != null)
                corr.done(req_id);
            completed.signalAll();
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean isCancelled() {
        lock.lock();
        try {
            return done;
        }
        finally {
            lock.unlock();
        }
    }


    public boolean isDone() {
        return done;
    }


    public String toString() {
        StringBuilder ret=new StringBuilder(128);
        ret.append(super.toString());
        ret.append("req_id=").append(req_id).append(", mode=" + options.getMode());
        return ret.toString();
    }


    /* --------------------------------- Private Methods -------------------------------------*/


    protected void checkCompletion(Future future) {
        if(listener != null && responsesComplete())
            listener.futureDone(future);
    }

    /** Generates a new unique request ID */
    protected static long getRequestId() {
        return REQUEST_ID.incrementAndGet();
    }

    /** This method runs with lock locked (called by <code>execute()</code>). */
    @GuardedBy("lock")
    protected boolean responsesComplete(long timeout) throws InterruptedException {
        if(timeout <= 0) {
            while(!done) { /* Wait for responses: */
                if(responsesComplete()) {
                    if(corr != null)
                        corr.done(req_id);
                    return true;
                }
                completed.await();
            }
            return responsesComplete();
        }
        else {
            long start_time=System.currentTimeMillis();
            long timeout_time=start_time + timeout;
            while(timeout > 0 && !done) { /* Wait for responses: */
                if(responsesComplete()) {
                    if(corr != null)
                        corr.done(req_id);
                    return true;
                }
                timeout=timeout_time - System.currentTimeMillis();
                if(timeout > 0) {
                    completed.await(timeout, TimeUnit.MILLISECONDS);
                }
            }
            if(corr != null)
                corr.done(req_id);
            return responsesComplete();
        }
    }

    @GuardedBy("lock")
    protected boolean waitForResults(long timeout)  {
        if(timeout <= 0) {
            while(true) { /* Wait for responses: */
                if(responsesComplete())
                    return true;
                try {completed.await();} catch(Exception e) {}
            }
        }
        else {
            long start_time=System.currentTimeMillis();
            long timeout_time=start_time + timeout;
            while(timeout > 0) { /* Wait for responses: */
                if(responsesComplete())
                    return true;
                timeout=timeout_time - System.currentTimeMillis();
                if(timeout > 0) {
                    try {completed.await(timeout, TimeUnit.MILLISECONDS);} catch(Exception e) {}
                }
            }
            return false;
        }
    }



    /*public static String modeToString(int m) {
        switch(m) {
            case GET_FIRST: return "GET_FIRST";
            case GET_ALL: return "GET_ALL";
            case GET_MAJORITY: return "GET_MAJORITY";
            case GET_ABS_MAJORITY: return "GET_ABS_MAJORITY";
            case GET_NONE: return "GET_NONE";
            default: return "<unknown> (" + m + ")";
        }
    }*/


}
package org.jgroups.groups.failure;

import org.jgroups.Address;
import org.jgroups.groups.MessageID;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author pedro
 *         Date: 25-04-2011
 */
public class FinalMessagesManager {
    private static final int TIMEOUT = 10 * 1000; // milliseconds == 1 minute

    //maps address of origin with their ids and final timestamps
    private final ConcurrentMap<Address, Info> finalMessages = new ConcurrentHashMap<Address, Info>();
    private final ConcurrentSkipListSet<Address> addressesToRemove = new ConcurrentSkipListSet<Address>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> gcHandler;

    public FinalMessagesManager() {
        gcHandler = scheduler.scheduleAtFixedRate(new GarbageCollection(), TIMEOUT, TIMEOUT, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        gcHandler.cancel(true);
    }

    public void start() {
        if(gcHandler.isCancelled() || gcHandler.isDone()) {
            gcHandler = scheduler.scheduleAtFixedRate(new GarbageCollection(), TIMEOUT, TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    public void setFinalTs(MessageID msgID, long ts) {
        Address origin = msgID.getAddress();
        long id = msgID.getId();
        Info info = get(origin);
        info.add(id, ts);
    }

    public void setDelivered(MessageID msgID) {
        Address origin = msgID.getAddress();
        long id = msgID.getId();
        Info info = get(origin);
        info.setDelivered(id);
    }

    public void deleteAddress(Address addr) {
        addressesToRemove.add(addr);
    }

    public Map<Long, Long> getFinalMessagesFrom(Address addr) {
        Info info = get(addr);
        return info.get();
    }

    private Info get(Address addr) {
        Info info = new Info();
        Info existing = finalMessages.putIfAbsent(addr, info);
        if(existing != null) {
            info = existing;
        }
        return info;
    }

    private class Info {
        //maps IDs to Final timestamps
        private ConcurrentMap<Long, Long> finalTimestamps;
        private ConcurrentMap<Long, Long> timeouts;

        public Info() {
            finalTimestamps = new ConcurrentHashMap<Long, Long>();
            timeouts = new ConcurrentHashMap<Long, Long>();
        }

        public void add(long id, long ts) {
            finalTimestamps.putIfAbsent(id, ts);
        }

        public void setDelivered(long id) {
            timeouts.put(id, System.currentTimeMillis() + TIMEOUT);
        }

        public Map<Long, Long> get() {
            return new HashMap<Long, Long>(finalTimestamps);
        }

        public void gc(long actualTime) {
            List<Long> toRemove = new LinkedList<Long>();
            for(Map.Entry<Long, Long> entry : timeouts.entrySet()) {
                long timeout = entry.getValue();
                long id = entry.getKey();
                if(timeout != -1 && timeout < actualTime) {
                    finalTimestamps.remove(id);
                    toRemove.add(id);
                }
            }
            timeouts.keySet().removeAll(toRemove);
        }
    }

    private class GarbageCollection implements Runnable {

        @Override
        public void run() {
            long actualTime = System.currentTimeMillis();
            removeAddresses();
            for(Info i : finalMessages.values()) {
                i.gc(actualTime);
            }
        }

        private void removeAddresses() {
            boolean finished = addressesToRemove.isEmpty();
            while(!finished) {
                Address addr = addressesToRemove.pollFirst();
                finalMessages.remove(addr);
                finished = addressesToRemove.isEmpty();
            }
        }
    }
}


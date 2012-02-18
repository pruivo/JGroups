package org.jgroups.groups.manager;

import org.jgroups.groups.MessageID;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author pedro
 *         Date: 31-05-2011
 */
public class DeliverManagerOld {
    private final ConcurrentSkipListMap<MapKey, Boolean> messages = new ConcurrentSkipListMap<MapKey, Boolean>(new MapKeyComparator());
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void update(MessageID id, long oldOrder, long newOrder) {
        try {
            lock.readLock().lock();
            MapKey m = new MapKey();
            m.id = id;
            m.order = oldOrder;

            Boolean b = messages.remove(m);

            m.order = newOrder;
            messages.put(m, (b != null ? b : false));
        } finally {
            lock.readLock().unlock();
        }
    }

    public void updateAndMarkCompleted(MessageID id, long oldOrder, long newOrder){
        try {
            lock.readLock().lock();
            MapKey m = new MapKey();
            m.id = id;
            m.order = oldOrder;
            messages.remove(m);

            m.order = newOrder;
            messages.put(m, true);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void markCompleted(MessageID id, long order) {
        try {
            lock.readLock().lock();
            MapKey m = new MapKey();
            m.id = id;
            m.order = order;
            messages.replace(m, true);
        } finally {
            lock.readLock().unlock();
        }
    }

    public MessageID getNextToDeliver() {
        try {
            lock.writeLock().lock();
            Map.Entry<MapKey, Boolean> entry = messages.firstEntry();
            if(entry != null && entry.getValue()) {
                messages.remove(entry.getKey());
                return entry.getKey().id;
            }
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        messages.clear();
    }

    public class MapKey {
        public MessageID id;
        public long order;

        @Override
        public String toString() {
            return "MapKey [id=" + id + ", order=" + order + "]";
        }


    }

    public class MapKeyComparator implements Comparator<MapKey> {

        @Override
        public int compare(MapKey o1, MapKey o2) {
            int idRes = o1.id.compareTo(o2.id);
            if(idRes == 0) {
                return 0;
            }
            if(o1.order == o2.order) {
                return idRes;
            }
            return new Long(o1.order).compareTo(o2.order);
        }
    }
}

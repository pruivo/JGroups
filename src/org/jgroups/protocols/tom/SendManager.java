package org.jgroups.protocols.tom;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class SendManager<K, E extends Comparable<E>> {

    public static final long NOT_READY = -1L;
    private final ConcurrentMap<K, KeyData<E>> map;

    public SendManager() {
        map = new ConcurrentHashMap<>();
    }

    public void put(K key, Collection<E> expectedElements, E localElement, long initialSequenceNumber) {
        KeyData<E> data = map.get(key);
        if (data == null) {
            data = new KeyData<>(expectedElements);
            KeyData<E> oldData = map.putIfAbsent(key, data);
            if (oldData != null) {
                data = oldData;
            }
        }

        data.addPropose(localElement, initialSequenceNumber);
    }

    public long addPropose(K key, E element, long sequenceNumber) {
        KeyData<E> data = map.get(key);
        if (data == null) {
            return NOT_READY;
        }
        return data.addPropose(element, sequenceNumber);
    }

    public boolean addDelivered(K key, E element) {
        KeyData<E> data = map.get(key);
        if (data == null) {
            return false;
        }
        if (data.addDelivered(element)) {
            map.remove(key);
            return true;
        }
        return false;
    }

    public List<E> getElements(K key) {
        KeyData<E> data = map.get(key);
        if (data == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(data.elements);
    }

    private static class KeyData<E extends Comparable<E>> {

        private final List<E> elements;
        private final BitSet proposed;
        private final BitSet delivered;
        private long sequenceNumber;

        private KeyData(Collection<E> elements) {
            TreeSet<E> unique = new TreeSet<>(elements);
            this.elements = new ArrayList<>(unique);
            final int size = elements.size();
            proposed = new BitSet(size);
            proposed.set(0, size - 1);
            delivered = new BitSet(size);
            delivered.set(0, size - 1);
            sequenceNumber = NOT_READY;
        }

        public long addPropose(E element, long sequenceNumber) {
            final int idx = Collections.binarySearch(elements, element);
            if (idx < 0) {
                //does not exists
                return NOT_READY;
            }
            synchronized (this) {
                this.sequenceNumber = Math.max(this.sequenceNumber, sequenceNumber);
                proposed.clear(idx);
                return proposed.isEmpty() ? this.sequenceNumber : NOT_READY;
            }
        }

        public boolean addDelivered(E element) {
            final int idx = Collections.binarySearch(elements, element);
            if (idx < 0) {
                //does not exists
                return false;
            }
            synchronized (this) {
                delivered.clear(idx);
                return proposed.isEmpty();
            }
        }

    }

}

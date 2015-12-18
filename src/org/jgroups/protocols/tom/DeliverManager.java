package org.jgroups.protocols.tom;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DeliverManager<K extends Comparable<K>, V> {

    private final Map<K, Node<K, V>> map;
    private final Lock lock;
    private final Condition ready;
    private Node<K, V> first;
    private Node<K, V> last;
    private long sequenceNumber;

    public DeliverManager() {
        map = new HashMap<>();
        lock = new ReentrantLock();
        ready = lock.newCondition();
        sequenceNumber = 0;
        first = null;
        last = null;
    }

    public long put(K key, V value, long sequenceNumber) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        try {
            lock.lock();
            Node<K, V> node = map.get(key);
            if (node != null) {
                //already exists
                return node.sequenceNumber;
            }
            doUpdateSequenceNumber(sequenceNumber);
            long thisNodeSequenceNumber = incrementSequenceNumber();
            node = new Node<>(key, value, thisNodeSequenceNumber);
            //should always be the last
            assert last == null || last.compareTo(node) < 0;
            insertLast(node);
            return thisNodeSequenceNumber;
        } finally {
            lock.unlock();
        }
    }

    public void markFinal(K key, long sequenceNumber) {
        Objects.requireNonNull(key);
        try {
            lock.lock();
            Node<K, V> node = map.get(key);
            if (node == null) {
                //already processed!
                return;
            }
            doUpdateSequenceNumber(sequenceNumber);
            //final sequence number must be always smaller or equals.
            assert node.sequenceNumber <= sequenceNumber;
            node.sequenceNumber = sequenceNumber;
            node.ready = true;
            if (node.next == null) {
                //last element, can't go up!
                return;
            }
            if (node.compareTo(node.next) > 0) {
                Node<K, V> before = node.next.next;
                //we need to update its position
                removeNotLast(node);
                while (before != null && node.compareTo(before) > 0) {
                    before = before.next;
                }
                if (before == null) {
                    insertLast(node);
                } else {
                    insertBefore(node, before);
                }
            }
        } finally {
            notifyFinal();
            lock.unlock();
        }
    }

    public void addReadyToDeliver(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        try {
            lock.lock();
            long thisNodeSequenceNumber = incrementSequenceNumber();
            Node<K, V> node = new Node<>(key, value, thisNodeSequenceNumber);
            node.ready = true;
            //should always be the last
            assert last == null || last.compareTo(node) < 0;
            insertLast(node);
        } finally {
            notifyFinal();
            lock.unlock();
        }
    }

    public void updateSequenceNumber(long sequenceNumber) {
        try {
            lock.lock();
            doUpdateSequenceNumber(sequenceNumber);
        } finally {
            lock.unlock();
        }

    }

    public int deliverReady(V[] values) throws InterruptedException {
        Objects.requireNonNull(values);
        if (values.length == 0) {
            throw new IllegalArgumentException("Values array must be non-empy");
        }
        try {
            lock.lock();
            while (!isFirstReady()) {
                ready.await();
            }
            Node<K, V> node = first;
            int added = 0;
            while (node != null && node.ready && added < values.length) {
                values[added++] = node.value;
                map.remove(node.key);
                node.previous = null;
                node = node.next;
                node.previous.next = null;
            }
            if (node == null) {
                //no more elements
                first = last = null;
            } else {
                node.previous = null;
                first = node;
            }
            return added;
        } finally {
            lock.unlock();
        }
    }

    private boolean isFirstReady() {
        return first != null && first.ready;
    }

    private void insertLast(Node<K, V> node) {
        if (last == null) {
            //no elements yet!
            first = last = node;
        } else {
            Node<K, V> previousNode = last;
            previousNode.next = node;
            node.previous = previousNode;
            node.next = null;
        }
    }

    private void insertBefore(Node<K, V> node, Node<K, V> next) {
        Node<K, V> previous = next.previous;

        //previous can never be null.
        assert previous != null;

        previous.next = node;
        next.previous = node;
        node.previous = previous;
        node.next = next;
    }

    private void removeNotLast(Node<K, V> node) {
        Node<K, V> previous = node.previous;
        Node<K, V> next = node.next;

        //not last as the method says
        assert next != null;

        if (previous == null) {
            //it is the first element!
            first = next;
            next.previous = null;
        } else {
            previous.next = next;
            next.previous = previous;
        }
    }

    private void notifyFinal() {
        if (isFirstReady()) {
            ready.signal();
        }
    }

    private void doUpdateSequenceNumber(long otherSequenceNumber) {
        sequenceNumber = Math.max(sequenceNumber, otherSequenceNumber);
    }

    private long incrementSequenceNumber() {
        return sequenceNumber++;
    }

    private static class Node<K extends Comparable<K>, V> {
        private final K key;
        private final V value;
        private long sequenceNumber;
        private boolean ready;
        private Node<K, V> previous;
        private Node<K, V> next;

        private Node(K key, V value, long initialSequenceNumber) {
            this.key = key;
            this.value = value;
            this.sequenceNumber = initialSequenceNumber;
            this.ready = false;
            this.previous = null;
            this.next = null;
        }

        public int compareTo(Node<K, V> otherNode) {
            int sequenceCompare = Long.compare(sequenceNumber, otherNode.sequenceNumber);
            return sequenceCompare == 0 ? key.compareTo(otherNode.key) : sequenceCompare;
        }
    }
}

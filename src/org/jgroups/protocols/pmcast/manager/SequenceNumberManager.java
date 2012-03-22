package org.jgroups.protocols.pmcast.manager;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class SequenceNumberManager {

    private long sequenceNumber = 0;

    public synchronized long getAndIncrement() {
        return sequenceNumber++;
    }

    public synchronized void update(long otherSequenceNumber) {
        sequenceNumber = Math.max(sequenceNumber, otherSequenceNumber + 1);
    }

    public synchronized long updateAndGet(long otherSequenceNumber) {
        if (sequenceNumber >= otherSequenceNumber) {
            return sequenceNumber++;
        } else {
            sequenceNumber = otherSequenceNumber + 1;
            return otherSequenceNumber;
        }
    }
}

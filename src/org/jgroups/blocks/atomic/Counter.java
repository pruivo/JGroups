package org.jgroups.blocks.atomic;

import static org.jgroups.util.CompletableFutures.join;

import java.util.concurrent.CompletableFuture;

/**
 * @author Bela Ban
 * @since 3.0.0
 */
public interface Counter {

    String getName();

    /**
     * Gets the current value of the counter
     * @return The current value
     */
    default long get() {
        return join(getAsync());
    }

    CompletableFuture<Long> getAsync();

    /**
     * Sets the counter to a new value
     * @param new_value The new value
     */
    default void set(long new_value) {
        join(setAsync(new_value));
    }

    CompletableFuture<Void> setAsync(long new_value);

    /**
     * Atomically updates the counter using a CAS operation
     *
     * @param expect The expected value of the counter
     * @param update The new value of the counter
     * @return True if the counter could be updated, false otherwise
     */
    default boolean compareAndSet(long expect, long update) {
        return join(compareAndSetAsync(expect, update));
    }

    default CompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        return compareAndSwapAsync(expect, update).thenApply(aLong -> aLong == expect);
    }

    default long compareAndSwap(long expect, long update) {
        return join(compareAndSwapAsync(expect, update));
    }

    CompletableFuture<Long> compareAndSwapAsync(long expect, long update);

    /**
     * Atomically increments the counter and returns the new value
     * @return The new value
     */
    default long incrementAndGet() {
        return join(incrementAndGetAsync());
    }

    default CompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    /**
     * Atomically decrements the counter and returns the new value
     * @return The new value
     */
    default long decrementAndGet() {
        return join(decrementAndGetAsync());
    }

    default CompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }


    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    default long addAndGet(long delta) {
       return join(addAndGetAsync(delta));
    }

    CompletableFuture<Long> addAndGetAsync(long delta);
}


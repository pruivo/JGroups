package org.jgroups.blocks.atomic;

import java.util.concurrent.CompletionStage;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface PNCounter {

    String getName();

    long get();

    CompletionStage<Void> add(long value);

    default CompletionStage<Void> increment() {
        return add(1);
    }

    default CompletionStage<Void> decrement() {
        return add(-1);
    }

}

package org.jgroups.pncounter;

import java.util.concurrent.CompletionStage;

/**
 * TODO! document this
 */
public interface PNCounterProtocol {
    CompletionStage<Void> updateAllMembers(String name, PNCounterStateSnapshot snapshot);
}

package org.jgroups.pncounter;

import org.jgroups.Address;

import java.util.concurrent.CompletionStage;

public class Request {
    public CompletionStage<Void> toCompletionStage() {
        return null;
    }

    public void onAck(Address src) {

    }
}

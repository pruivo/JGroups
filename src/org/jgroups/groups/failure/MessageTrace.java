package org.jgroups.groups.failure;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.HashMap;
import java.util.Map;

/**
 * @author pedro
 *         Date: 04-05-2011
 */
public class MessageTrace {
    private final MatrixClock matrixClock = new MatrixClock();
    private final Map<Address, Map<Long, Message>> messages = new HashMap<Address, Map<Long, Message>>();

    public MessageTrace() {}

    public void traceMessage(Address self, Message msg, long seqno) {
        Address from = msg.getSrc();
        matrixClock.seeMessage(self, from, seqno);
        Map<Long, Message> oldmsgs = messages.get(from);
        if(oldmsgs == null) {
            oldmsgs = new HashMap<Long, Message>();
            messages.put(from, oldmsgs);
        }
        oldmsgs.put(seqno, msg);
    }
}

package org.jgroups.groups;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Pedro
 */
public class CoordinatorInfoMessage implements Serializable{

    private Map<MessageID, Long> delivered;

    public CoordinatorInfoMessage() {
        this.delivered = new HashMap<MessageID, Long>();
    }

    public void putDelivered(MessageID msgID, long timestamp) {
        delivered.put(msgID, timestamp);
    }

    public Map<MessageID, Long> getDelivered() {
        return delivered;
    }
}

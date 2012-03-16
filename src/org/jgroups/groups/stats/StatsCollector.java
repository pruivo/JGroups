package org.jgroups.groups.stats;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class StatsCollector {
    
    private static enum Counter {
        PROPOSE_MESSAGE,
        LAST_PROPOSE_MESSAGE,
        FINAL_MESSAGE,
        DATA_MESSAGE,
        GROUP_MESSAGE_SENT,
        GROUP_MESSAGE_DELIVERED,
        UNICAST_MESSAGE_SENT        
    }
    
    private static enum Duration {
        PROPOSE_MESSAGE,
        LAST_PROPOSE_MESSAGE,
        FINAL_MESSAGE,
        DATA_MESSAGE,
        GROUP_MESSAGE_SENT
    }
    
    //from javadoc: Enum maps are represented internally as arrays. This representation is extremely compact and efficient. 
    //this way is simple to add new stats and avoids create N field with atomic long or atomic integer. 
    private EnumMap<Counter, AtomicInteger> counters;    
    private EnumMap<Duration, AtomicLong> durations;
    
    public StatsCollector() {
        counters = new EnumMap<Counter, AtomicInteger>(Counter.class);
        durations = new EnumMap<Duration, AtomicLong>(Duration.class);
        
        for (Counter counter : Counter.values()) {
            counters.put(counter, new AtomicInteger(0));
        }
        
        for (Duration duration : Duration.values()) {
            durations.put(duration, new AtomicLong(0));
        }               
    }
    
    private volatile boolean statsEnabled;
    
    private boolean shouldCollectStats(long... values) {
        if (!statsEnabled) {
            return false;
        }
        for (long value : values) {
            if (value < 0) {
                return false;
            }
        }
        return true;
    }
    
    private double convertNanosToMillis(long nanos) {
        return nanos / 1000000.0;
    }
    
    public void setStatsEnabled(boolean statsEnabled) {
        this.statsEnabled = statsEnabled;
    }
    
    public void clearStats() {
        for (AtomicInteger counter : counters.values()) {
            counter.set(0);
        }
        
        for (AtomicLong duration : durations.values()) {
            duration.set(0);
        }
    }
    
    public long now() {
        return statsEnabled ? System.nanoTime() : -1;
    }        
     
    public void addProposeSequenceNumberDuration(long durationValue, boolean isLastProposeMessage) {
        if (!shouldCollectStats(durationValue)) {
            return;
        }
        Counter counter = isLastProposeMessage ? Counter.LAST_PROPOSE_MESSAGE : 
                Counter.PROPOSE_MESSAGE;
        Duration duration = isLastProposeMessage ? Duration.LAST_PROPOSE_MESSAGE :
                Duration.PROPOSE_MESSAGE;
        
        counters.get(counter).incrementAndGet();
        durations.get(duration).addAndGet(durationValue);
    }
    
    public void addFinalSequenceNumberDuration(long duration) {
        if (!shouldCollectStats(duration)) {
            return;
        }        
        
        counters.get(Counter.FINAL_MESSAGE).incrementAndGet();
        durations.get(Duration.FINAL_MESSAGE).addAndGet(duration);
    }
    
    public void addDataMessageDuration(long duration) {
        if (!shouldCollectStats(duration)) {
            return;
        }
        
        counters.get(Counter.DATA_MESSAGE).incrementAndGet();
        durations.get(Duration.DATA_MESSAGE).addAndGet(duration);
    }
    
    public void addGroupMulticastSentDuration(long duration, int numberOfUnicasts) {
        if (!shouldCollectStats(duration)) {
            return;
        }
        
        counters.get(StatsCollector.Counter.UNICAST_MESSAGE_SENT).addAndGet(numberOfUnicasts);
        counters.get(Counter.GROUP_MESSAGE_SENT).incrementAndGet();
        durations.get(Duration.GROUP_MESSAGE_SENT).addAndGet(duration);
    }
    
    public void incrementMessageDeliver() {
        if (!shouldCollectStats()) {
            return ;
        }
        counters.get(Counter.GROUP_MESSAGE_DELIVERED).incrementAndGet();
    }
    
    public double getAvgDataMessageReceivedDuration() {
        int count = counters.get(StatsCollector.Counter.DATA_MESSAGE).get();
        if (count > 0) {
            long dur = durations.get(Duration.DATA_MESSAGE).get();
            return convertNanosToMillis(dur) / count;
        }
        return 0D;
    }       
    
    public double getAvgGroupMulticastSentDuration() {
        int count = counters.get(StatsCollector.Counter.GROUP_MESSAGE_SENT).get();
        if (count > 0) {
            long dur = durations.get(Duration.GROUP_MESSAGE_SENT).get();
            return convertNanosToMillis(dur) / count;
        }
        return 0D;
    }
    
    public double getAvgProposeMesageReceivedDuration() {
        int count = counters.get(StatsCollector.Counter.PROPOSE_MESSAGE).get();
        if (count > 0) {
            long dur = durations.get(Duration.PROPOSE_MESSAGE).get();
            return convertNanosToMillis(dur) / count;
        }
        return 0D;
    }
    
    public double getAvgLastProposeMessageReceivedDuration() {
        int count = counters.get(StatsCollector.Counter.LAST_PROPOSE_MESSAGE).get();
        if (count > 0) {
            long dur = durations.get(Duration.LAST_PROPOSE_MESSAGE).get();
            return convertNanosToMillis(dur) / count;
        }
        return 0D;
    }
    
    public double getAvgFinalMessageReceivedDuration() {
        int count = counters.get(StatsCollector.Counter.FINAL_MESSAGE).get();
        if (count > 0) {
            long dur = durations.get(Duration.FINAL_MESSAGE).get();
            return convertNanosToMillis(dur) / count;
        }
        return 0D;
    }
    
    public int getNumberOfGroupMulticastMessagesSent() {
        return counters.get(StatsCollector.Counter.DATA_MESSAGE).get(); 
    }
    
    public int getGroupMulticastDelivered() {
        return counters.get(StatsCollector.Counter.GROUP_MESSAGE_DELIVERED).get();
    }
    
    public int getNumberOfProposeMessagesReceived() {
        return counters.get(StatsCollector.Counter.PROPOSE_MESSAGE).get() +
                counters.get(StatsCollector.Counter.LAST_PROPOSE_MESSAGE).get();
    }
    
    public double getAvgNumberOfUnicastSentPerGroupMulticast() {
        int multicast = counters.get(StatsCollector.Counter.GROUP_MESSAGE_SENT).get();
        if (multicast > 0) {
            int unicast = counters.get(StatsCollector.Counter.UNICAST_MESSAGE_SENT).get();
            return unicast * 1.0 / multicast;
        }
        return 0D;
    }
}

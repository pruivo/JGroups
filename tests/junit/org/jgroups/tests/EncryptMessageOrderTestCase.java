package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Iterator;
import java.util.Vector;

/**
 * This test case checks ordering of messages using the Encrypt protocol
 * using <code>UDP</code>protocols. It can be run
 * as JUnit test case or in the command line. Parameters are:
 * <ul>
 * <li><code>-sleep n</code> - means that after each message sending, sender
 * thread will sleep for <code>n</code> milliseconds;
 * <li><code>-msg_num n</code> - <code>n</code> is number of messages to send;
 * <li><code>-debug</code> - pop-up protocol debugger;
 * </ul>
 */
@Test(groups={Global.STACK_INDEPENDENT,"broken"},sequential=true)
public class EncryptMessageOrderTestCase {

    public static int MESSAGE_NUMBER=5 * 100;

    public static boolean SLEEP_BETWEEN_SENDING=false;

    public static int SLEEP_TIME=1;

    String groupName = "ENCRYPT_ORDER_TEST";

    static final short ID=100;

    boolean orderCounterFailure = false;

    public static final String properties ="EncryptNoKeyStore.xml";



    protected JChannel channel1;
    protected JChannel channel2;

    /**
     * Print selected options before test starts.
     */
    protected static void printSelectedOptions() {
        System.out.println("will sleep : " + SLEEP_BETWEEN_SENDING);
        if(SLEEP_BETWEEN_SENDING)
            System.out.println("sleep time : " + SLEEP_TIME);

        System.out.println("msg num : " + MESSAGE_NUMBER);


    }

    @BeforeClass
    static void init() {
        ClassConfigurator.add((short)24526, EncryptOrderTestHeader.class);
    }

    /**
     * Set up unit test. It might add protocol
     * stack debuggers if such option was selected at startup.
     */
    @BeforeMethod
    protected void setUp() throws Exception {
        printSelectedOptions();

        channel1=new JChannel(properties);
        System.out.print("Connecting to channel...");
        channel1.connect(groupName);
        System.out.println("channel1 connected, view is " + channel1.getView());

        channel2=new JChannel(properties);
        channel2.connect(groupName);
        System.out.println("channel2 connected, view is " + channel2.getView());
    }

    /**
     * Tears down test case. This method closes all opened channels.
     */
    @AfterMethod
    protected void tearDown() throws Exception {
        channel2.close();
        channel1.close();
    }

    protected volatile boolean finishedReceiving;

    /**
     * Test method. This method adds a message listener to the PullPushAdapter
     * on channel 1, and starts sending specified number of messages into
     * channel 1 or 2 depending if we are in loopback mode or not. Each message
     * containg timestamp when it was created. By measuring time on message
     * delivery we can calculate message trip time. Listener is controlled by
     * two string messages "start" and "stop". After sender has finished to
     * send messages, it waits until listener receives all messages or "stop"
     * message. Then test is finished and calculations are showed.
     * <p/>
     * Also we calculate how much memory
     * was allocated before excuting a test and after executing a test.
     */
    @Test
    public void testLoad() {
        try {
            final String startMessage="start";
            final String stopMessage="stop";

            final Object mutex=new Object();

            final Vector<Long> receivedTimes=new Vector<Long>(MESSAGE_NUMBER);
            final Vector<Object> normalMessages=new Vector<Object>(MESSAGE_NUMBER);
            final Vector<Object> tooQuickMessages=new Vector<Object>();
            final Vector<Object> tooSlowMessages=new Vector<Object>();

            channel1.setReceiver(new ReceiverAdapter() {
                private boolean started=false;
                private boolean stopped=false;

                private long counter = 0L;
                
                public void receive(Message jgMessage) {
                    Object message=jgMessage.getObject();

                    if(startMessage.equals(message)) {
                        started=true;
                        finishedReceiving=false;
                    }
                    else if(stopMessage.equals(message)) {
                        stopped=true;
                        finishedReceiving=true;

                        synchronized(mutex) {
                            mutex.notifyAll();
                        }

                    }
                    else if(message instanceof Long) {
                        Long travelTime=new Long(System.currentTimeMillis() - ((Long)message).longValue());

                        try {
                            Assert.assertEquals(counter, ((EncryptOrderTestHeader)jgMessage.getHeader(ID)).seqno);
                        	counter++;
                        } catch (Exception e){
                        	e.printStackTrace();
                        	orderCounterFailure =true;
                        }
                        if(!started)
                            tooQuickMessages.add(message);
                        else if(!stopped) {
                            receivedTimes.add(travelTime);
                            normalMessages.add(message);
                        }
                        else
                            tooSlowMessages.add(message);
                    }
                }
            });

            System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());
            System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());
            System.out.println("Starting sending messages.");

            long time=System.currentTimeMillis();

            Message startJgMessage=new Message();
            startJgMessage.setObject(startMessage);

            JChannel sender= channel2;

            sender.send(startJgMessage);

            for(int i=0; i < MESSAGE_NUMBER; i++) {
                Long message=new Long(System.currentTimeMillis());
                Message jgMessage=new Message();
                jgMessage.putHeader(ID, new EncryptOrderTestHeader(i));
                jgMessage.setObject(message);
                sender.send(jgMessage);

                if(i % 1000 == 0)
                    System.out.println("sent " + i + " messages.");

                if(SLEEP_BETWEEN_SENDING)
                    org.jgroups.util.Util.sleep(1, true);
            }

            Message stopJgMessage=new Message();
            stopJgMessage.setObject(stopMessage);
            sender.send(stopJgMessage);

            time=System.currentTimeMillis() - time;

            System.out.println("Finished sending messages. Operation took " + time);

            synchronized(mutex) {

                int received=0;

                while(!finishedReceiving) {
                    mutex.wait(1000);

                    if(receivedTimes.size() != received) {
                        received=receivedTimes.size();
                        System.out.println();
                        System.out.print("Received " + receivedTimes.size() + " messages.");
                    }
                    else {
                        System.out.print(".");
                    }
                }
            }

            try {
                Thread.sleep(1000);
            }
            catch(Exception ex) {
            }

            double avgDeliveryTime=-1.0;
            long maxDeliveryTime=Long.MIN_VALUE;
            long minDeliveryTime=Long.MAX_VALUE;

            Iterator<Long> iterator=receivedTimes.iterator();
            while(iterator.hasNext()) {
                Long message=iterator.next();

                if(avgDeliveryTime == -1.0)
                    avgDeliveryTime=message.longValue();
                else
                    avgDeliveryTime=(avgDeliveryTime + message.doubleValue()) / 2.0;

                if(message.longValue() > maxDeliveryTime)
                    maxDeliveryTime=message.longValue();

                if(message.longValue() < minDeliveryTime)
                    minDeliveryTime=message.longValue();
            }

            System.out.println("Sent " + MESSAGE_NUMBER + " messages.");
            System.out.println("Received " + receivedTimes.size() + " messages.");
            System.out.println("Average delivery time " + avgDeliveryTime + " ms");
            System.out.println("Minimum delivery time " + minDeliveryTime + " ms");
            System.out.println("Maximum delivery time " + maxDeliveryTime + " ms");
            System.out.println("Received " + tooQuickMessages.size() + " too quick messages");
            System.out.println("Received " + tooSlowMessages.size() + " too slow messages");
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }

        System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());
        System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());

        System.out.println("Performing GC");

        Runtime.getRuntime().gc();

        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException ex) {
        }

        System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());
        System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());

        assert (!orderCounterFailure) : "Message ordering is incorrect - check log output";
    }
    
    


    static void help() {
        System.out.println("EncryptOrderTest [-help] [-sleep <sleep time between sends (ms)>] " +
                " [-msg_num <number of msgs to send>]");
    }
    
    public static class EncryptOrderTestHeader extends Header{
        long seqno = -1; // either reg. NAK_ACK_MSG or first_seqno in retransmissions

        public EncryptOrderTestHeader() {
        }

        public EncryptOrderTestHeader(long seqno){
            this.seqno = seqno;
        }

        public int size(){
            return Global.LONG_SIZE;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeLong(seqno);
        }

        public void readFrom(DataInput in) throws Exception {
            seqno=in.readLong();
        }

        public EncryptOrderTestHeader copy(){
            return new EncryptOrderTestHeader(seqno);
        }

        public String toString(){
            StringBuilder ret = new StringBuilder();
            ret.append("[ENCRYPT_ORDER_TEST: seqno=" + seqno);
            ret.append(']');

            return ret.toString();
        }

    }

}

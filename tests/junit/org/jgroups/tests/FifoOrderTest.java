package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the concurrent stack (TP)
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class FifoOrderTest extends ChannelTestBase {    
    JChannel ch1, ch2, ch3;
    final static int NUM=25, EXPECTED=NUM * 3;
    final static long SLEEPTIME=100;
    CyclicBarrier barrier;


    @BeforeMethod
    void setUp() throws Exception {
        barrier=new CyclicBarrier(4);
        ch1=createChannel(true,3, "A");
        ch2=createChannel(ch1,    "B");
        ch3=createChannel(ch1,    "C");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(ch3, ch2, ch1);
        barrier.reset();
    }


    @Test
    public void testFifoDelivery() throws Exception {
        long start, stop, diff;
        modifyDefaultThreadPool(ch1);
        modifyDefaultThreadPool(ch2);
        modifyDefaultThreadPool(ch3);

        MyReceiver r1=new MyReceiver("R1"), r2=new MyReceiver("R2"), r3=new MyReceiver("R3");
        ch1.setReceiver(r1); ch2.setReceiver(r2); ch3.setReceiver(r3);

        ch1.connect("FifoOrderTest");
        ch2.connect("FifoOrderTest");
        ch3.connect("FifoOrderTest");
        View v=ch3.getView();
        assert v.size() == 3 : "view is " + v;

        new Thread(new Sender(ch1)) {}.start();
        new Thread(new Sender(ch2)) {}.start();
        new Thread(new Sender(ch3)) {}.start();
        barrier.await(); // start senders
        start=System.currentTimeMillis();

        Exception ex=null;

        try {
            System.out.println("waiting for all messages to be received");
            barrier.await((long)(EXPECTED * SLEEPTIME * 1.5), TimeUnit.MILLISECONDS); // wait for all receivers
        }
        catch(java.util.concurrent.TimeoutException e) {
            ex=e;
        }


        stop=System.currentTimeMillis();
        diff=stop - start;

        System.out.println("Total time: " + diff + " ms\n");

        checkFIFO(r1);
        checkFIFO(r2);
        checkFIFO(r3);
        if(ex != null)
            throw ex;
    }

    private void checkFIFO(MyReceiver r) {
        Map<Address,List<Integer>> map=r.getMessages();

        boolean fifo=true;
        List<Address> incorrect_receivers=new LinkedList<Address>();
        System.out.println("Checking FIFO for " + r.getName() + ":");
        for(Address addr: map.keySet()) {
            List<Integer> list=map.get(addr);
            print(addr, list);
            if(!verifyFIFO(list)) {
                fifo=false;
                incorrect_receivers.add(addr);
            }
        }
        System.out.print("\n");

        if(!fifo)
            assert false : "The following receivers didn't receive all messages in FIFO order: " + incorrect_receivers;
    }


    private static boolean verifyFIFO(List<Integer> list) {
        List<Integer> list2=new LinkedList<Integer>(list);
        Collections.sort(list2);
        return list.equals(list2);
    }

    private static void print(Address addr, List<Integer> list) {
        StringBuilder sb=new StringBuilder();
        sb.append(addr).append(": ");
        for(Integer i: list)
            sb.append(i).append(" ");
        System.out.println(sb);
    }



    private static void modifyDefaultThreadPool(JChannel ch1) {
        TP transport=ch1.getProtocolStack().getTransport();
        ThreadPoolExecutor default_pool=(ThreadPoolExecutor)transport.getDefaultThreadPool();
        if(default_pool != null) {
            default_pool.setCorePoolSize(1);
            default_pool.setMaximumPoolSize(100);
        }
        transport.setThreadPoolQueueEnabled(false);
    }


    private class Sender implements Runnable {
        final Channel ch;
        final Address local_addr;

        public Sender(Channel ch) {
            this.ch=ch;
            local_addr=ch.getAddress();
        }

        public void run() {
            Message msg;
            try {
                barrier.await();
            }
            catch(Throwable t) {
                return;
            }

            for(int i=1; i <= NUM; i++) {
                msg=new Message(null, null, new Integer(i));
                try {                    
                    ch.send(msg);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private class Pair<K,V> {
        K key;
        V val;

        public Pair(K key, V val) {
            this.key=key;
            this.val=val;
        }

        public String toString() {
            return key + "::" + val;
        }
    }

    private class MyReceiver extends ReceiverAdapter {
        String name;
        final ConcurrentMap<Address,List<Integer>> msgs=new ConcurrentHashMap<Address,List<Integer>>();

        AtomicInteger count=new AtomicInteger(0);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            Util.sleep(SLEEPTIME);

            Address sender=msg.getSrc();
            List<Integer> list=msgs.get(sender);
            if(list == null) {
                list=new LinkedList<Integer>();
                List<Integer> tmp=msgs.putIfAbsent(sender, list);
                if(tmp != null)
                    list=tmp;
            }
            Integer num=(Integer)msg.getObject();
            list.add(num); // no concurrent access: FIFO per sender ! (No need to synchronize on list)

            if(count.incrementAndGet() >= EXPECTED) {
                System.out.println("[" + name + "]: received all messages (" + count.get() + ")");
                try {
                    barrier.await();
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public ConcurrentMap<Address,List<Integer>> getMessages() {return msgs;}

        public String getName() {
            return name;
        }
    }


}

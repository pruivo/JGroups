
package org.jgroups.tests;


import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Global;
import org.jgroups.ReceiverAdapter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Tests sending and receiving of messages within the same VM. Sends N messages
 * and expects reception of N messages within a given time. Fails otherwise.
 * @author Bela Ban
 */
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class SendAndReceiveTest {
    JChannel channel;
    static final int NUM_MSGS=1000;
    static final long TIMEOUT=30000;

    String props1="UDP(loopback=true;mcast_port=27000;ip_ttl=1;" +
            "mcast_send_buf_size=64000;mcast_recv_buf_size=64000):" +
            //"PIGGYBACK(max_wait_time=100;max_size=32000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400,4800):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=8096):" +
            "pbcast.GMS(join_timeout=5000;" +
            "print_local_addr=true)";

        String props2="UDP(loopback=false;mcast_port=27000;ip_ttl=1;" +
                "mcast_send_buf_size=64000;mcast_recv_buf_size=64000):" +
                //"PIGGYBACK(max_wait_time=100;max_size=32000):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(retransmit_timeout=300,600,1200,2400,4800):" +
                "UNICAST(timeout=600,1200,2400,4800):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=8096):" +
                "pbcast.GMS(join_timeout=5000;" +
                "print_local_addr=true)";

    String props3="SHARED_LOOPBACK:" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400,4800):" +
             "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=8096):" +
            "pbcast.GMS(join_timeout=5000;" +
            "print_local_addr=true)";




    private void setUp(String props) {
        try {
            channel=new JChannel(props);
            channel.connect("test1");
        }
        catch(Throwable t) {
            t.printStackTrace(System.err);
            assert false : "channel could not be created";
        }
    }


    @AfterMethod
    public void tearDown() {
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    /**
     * Sends NUM messages and expects NUM messages to be received. If
     * NUM messages have not been received after 20 seconds, the test failed.
     */
    @Test
    public void testSendAndReceiveWithDefaultUDP_Loopback() {
        setUp(props1);
        MyReceiver receiver=new MyReceiver();
        channel.setReceiver(receiver);
        sendMessages(NUM_MSGS);
        int received_msgs=receiveMessages(receiver, NUM_MSGS, TIMEOUT);
        assert received_msgs >= NUM_MSGS;
    }

    @Test
    public void testSendAndReceiveWithDefaultUDP_NoLoopback() {
        setUp(props2);
        MyReceiver receiver=new MyReceiver();
        channel.setReceiver(receiver);
        sendMessages(NUM_MSGS);
        int received_msgs=receiveMessages(receiver, NUM_MSGS, TIMEOUT);
        assert received_msgs >= NUM_MSGS;
    }

    @Test
    public void testSendAndReceiveWithLoopback() {
        setUp(props3);
        MyReceiver receiver=new MyReceiver();
        channel.setReceiver(receiver);
        sendMessages(NUM_MSGS);
        int received_msgs=receiveMessages(receiver, NUM_MSGS, TIMEOUT);
        assert received_msgs >= NUM_MSGS;
    }

    private void sendMessages(int num) {
        Message msg;
        for(int i=0; i < num; i++) {
            try {
                msg=new Message();
                channel.send(msg);
                System.out.print(i + " ");
            }
            catch(Throwable t) {
                assert false : "could not send message #" + i;
            }
        }
    }


    /**
     * Receive at least <tt>num</tt> messages. Total time should not exceed <tt>timeout</tt>
     * @param num
     * @param timeout Must be > 0
     * @return
     */
    private static int receiveMessages(MyReceiver receiver, int num, long timeout) {
        if(timeout <= 0)
            timeout=5000;

        long start=System.currentTimeMillis(), current, wait_time;
        while(true) {
            current=System.currentTimeMillis();
            wait_time=timeout - (current - start);
            if(wait_time <= 0)
                break;
            try {
                if(receiver.getReceived() >= num)
                    break;
            }
            catch(Throwable t) {
                assert false : "failed receiving message";
            }
        }
        return receiver.getReceived();
    }


    protected static class MyReceiver extends ReceiverAdapter {
        int received=0;

        public int getReceived() {
            return received;
        }

        public void receive(Message msg) {
            received++;
            System.out.print("+" + received + ' ');
        }
    }


}

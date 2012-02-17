
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;


/**
 * Tests the UNICAST protocol
 * @author Bela Ban
 * @version $id$
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class UNICAST_Test {
    JChannel ch;
    Address a1;

    static final int SIZE=1000; // bytes
    static final int NUM_MSGS=10000;


    @AfterMethod
    void tearDown() throws Exception {
        Util.close(ch);
    }

    @Test(dataProvider="configProvider")
    public void testReceptionOfAllMessages(Protocol prot) throws Throwable {
        System.out.println("prot=" + prot.getClass().getSimpleName());
        ch=createChannel(prot, null);
        ch.connect("UNICAST_Test.testReceptionOfAllMessages");
        _testReceptionOfAllMessages();
    }


    @Test(dataProvider="configProvider")
    public void testReceptionOfAllMessagesWithDISCARD(Protocol prot) throws Throwable {
        System.out.println("prot=" + prot.getClass().getSimpleName());
        DISCARD discard=new DISCARD();
        discard.setDownDiscardRate(0.1); // discard all down message with 10% probability
        ch=createChannel(prot, discard);
        ch.connect("UNICAST_Test.testReceptionOfAllMessagesWithDISCARD");
        _testReceptionOfAllMessages();
    }

    @DataProvider
    public static Object[][] configProvider() {
        Object[][] retval=new Object[][] {
                {new UNICAST()}, {new UNICAST2()}
        };

        ((UNICAST)retval[0][0]).setTimeout(new int[]{500,1000,2000,3000});
        ((UNICAST2)retval[1][0]).setTimeout(new int[]{500,1000,2000,3000});
        return retval;
    }



    private static byte[] createPayload(int size, int seqno) {
        ByteBuffer buf=ByteBuffer.allocate(size);
        buf.putInt(seqno);
        return buf.array();
    }

    protected static JChannel createChannel(Protocol unicast, DISCARD discard) throws Exception {
        JChannel ch=new JChannel(false);
        ProtocolStack stack=new ProtocolStack();
        ch.setProtocolStack(stack);
        stack.addProtocol(new SHARED_LOOPBACK());

        if(discard != null)
            stack.addProtocol(discard);
        
        stack.addProtocol(new PING())
          .addProtocol(new NAKACK().setValue("use_mcast_xmit", false))
          .addProtocol(unicast)
          .addProtocol(new STABLE().setValue("max_bytes", 50000))
          .addProtocol(new GMS().setValue("print_local_addr", false))
          .addProtocol(new UFC())
          .addProtocol(new MFC())
          .addProtocol(new FRAG2());
        stack.init();
        return ch;
    }


    /** Checks that messages 1 - NUM_MSGS are received in order */
    static class Receiver extends ReceiverAdapter {
        int num_mgs_received=0, next=1;
        Throwable exception=null;
        boolean received_all=false;

        public void receive(Message msg) {
            if(exception != null)
                return;
            ByteBuffer buf=ByteBuffer.wrap(msg.getRawBuffer());
            int seqno=buf.getInt();
            if(seqno != next) {
                exception=new Exception("expected seqno was " + next + ", but received " + seqno);
                return;
            }
            next++;
            num_mgs_received++;
            if(num_mgs_received % 1000 == 0)
                System.out.println("<== " + num_mgs_received);
            if(num_mgs_received == NUM_MSGS) {
                synchronized(this) {
                    received_all=true;
                    this.notifyAll();
                }
            }
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }

        public boolean receivedAll() {return received_all;}

        public Throwable getException() {
            return exception;
        }
    }


    private void _testReceptionOfAllMessages() throws Throwable {
        int num_received=0;
        final Receiver r=new Receiver();
        ch.setReceiver(r);

        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new Message(a1, null, createPayload(SIZE, i)); // unicast message
            ch.send(msg);
            if(i % 1000 == 0)
                System.out.println("==> " + i);
        }
        int num_tries=10;
        while((num_received=r.getNumberOfReceivedMessages()) != NUM_MSGS && num_tries > 0) {
            if(r.getException() != null)
                throw r.getException();
            synchronized(r) {
                try {r.wait(3000);}
                catch(InterruptedException e) {}
            }
            num_tries--;
        }
        printStats(num_received);
        Assert.assertEquals(num_received, NUM_MSGS);
    }


    private void printStats(int num_received) {
        System.out.println("-- num received=" + num_received);
    }



}

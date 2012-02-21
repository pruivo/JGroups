
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * Demos the creation of a channel and subsequent connection and closing. Demo application should exit (no
 * more threads running)
 */
@Test(groups=Global.STACK_DEPENDENT, sequential=false)
public class CloseTest extends ChannelTestBase {
    private final ThreadLocal<JChannel> ch=new ThreadLocal<JChannel>(),
            channel1=new ThreadLocal<JChannel>(),
            c1=new ThreadLocal<JChannel>(),
            c2=new ThreadLocal<JChannel>(),
            c3=new ThreadLocal<JChannel>();


    @AfterMethod
    void tearDown() throws Exception {
        closeChannel(ch);
        closeChannel(channel1);
        closeChannel(c1);
        closeChannel(c2);
        closeChannel(c3);
    }

    protected boolean useBlocking() {
        return false;
    }


    private static void closeChannel(ThreadLocal<JChannel> local) {
        Channel c=local.get();
        if(c != null && (c.isOpen() || c.isConnected())) {
            c.close();
        }
        local.set(null);
    }


    public void testDoubleClose() throws Exception {
        System.out.println("-- creating channel1 --");
        channel1.set(createChannel(true));
        System.out.println("-- connecting channel1 --");
        channel1.get().connect(getUniqueClusterName("CloseTest.testDoubleClose"));

        assertTrue("channel open", channel1.get().isOpen());
        assertTrue("channel connected", channel1.get().isConnected());

        System.out.println("-- closing channel1 --");
        channel1.get().close();
        System.out.println("-- closing channel1 (again) --");
        channel1.get().close();
        assertFalse("channel not connected", channel1.get().isConnected());
    }

    public void testCreationAndClose() throws Exception {
        System.out.println("-- creating channel1 --");
        ch.set(createChannel(true));
        ch.get().connect(getUniqueClusterName("CloseTest.testCreationAndClose"));
        assertTrue("channel open", ch.get().isOpen());
        assertTrue("channel connected", ch.get().isConnected());
        ch.get().close();
        assertFalse("channel not connected", ch.get().isConnected());
        ch.get().close();
    }

    public void testViewChangeReceptionOnChannelCloseByParticipant() throws Exception {
        Address a1, a2;
        List<Address> members;
        MyReceiver r1=new MyReceiver(), r2=new MyReceiver();

        c1.set(createChannel(true));
        c1.get().setReceiver(r1);
        System.out.println("-- connecting c1");
        final String GROUP="CloseTest.testViewChangeReceptionOnChannelCloseByParticipant";
        c1.get().connect(GROUP);
        Util.sleep(500); // time to receive its own view
        System.out.println("c1: " + r1.getViews());
        a1=c1.get().getAddress();
        c2.set(createChannel(c1.get()));
        c2.get().setReceiver(r2);
        System.out.println("-- connecting c2");
        r1.clearViews();
        c2.get().connect(GROUP);
        Util.sleep(500); // time to receive its own view
        a2=c2.get().getAddress();
        System.out.println("c2: " + r2.getViews());

        System.out.println("-- closing c2");
        c2.get().close();
        Util.sleep(500);
        View v=r1.getViews().get(0);
        members=v.getMembers();
        System.out.println("-- first view of c1: " + v);
        Assert.assertEquals(2, members.size());
        assertTrue(members.contains(a1));
        assertTrue(members.contains(a2));

        v=r1.getViews().get(1);
        members=v.getMembers();
        System.out.println("-- second view of c1: " + v);
        assert 1 == members.size();
        assert members.contains(a1);
        assert !members.contains(a2);
    }

    public void testViewChangeReceptionOnChannelCloseByCoordinator() throws Exception {
        Address a1, a2;
        List<Address> members;
        MyReceiver r1=new MyReceiver(), r2=new MyReceiver();

        final String GROUP=getUniqueClusterName("CloseTest.testViewChangeReceptionOnChannelCloseByCoordinator");
        c1.set(createChannel(true));
        c1.get().setReceiver(r1);
        c1.get().connect(GROUP);
        Util.sleep(500); // time to receive its own view
        a1=c1.get().getAddress();
        c2.set(createChannel(c1.get()));
        c2.get().setReceiver(r2);
        c2.get().connect(GROUP);
        Util.sleep(500); // time to receive its own view
        a2=c2.get().getAddress();
        View v=r2.getViews().get(0);
        members=v.getMembers();
        assert 2 == members.size();
        assert members.contains(a2);

        r2.clearViews();
        c1.get().close();
        Util.sleep(1000);

        v=r2.getViews().get(0);
        members=v.getMembers();
        assert 1 == members.size();
        assert !members.contains(a1);
        assert members.contains(a2);
    }


    public void testConnectDisconnectConnectCloseSequence() throws Exception {
        System.out.println("-- creating channel --");
        ch.set(createChannel(true));

        System.out.println("-- connecting channel to CloseTest--");
        ch.get().connect("CloseTest.testConnectDisconnectConnectCloseSequence-CloseTest");
        System.out.println("view is " + ch.get().getView());

        System.out.println("-- disconnecting channel --");
        ch.get().disconnect();

        Util.sleep(500);
        System.out.println("-- connecting channel to OtherGroup --");
        ch.get().connect("CloseTest.testConnectDisconnectConnectCloseSequence-OtherGroup");
        System.out.println("view is " + ch.get().getView());

        System.out.println("-- closing channel --");
        ch.get().close();
    }


    

    public void testConnectCloseSequenceWith2Members() throws Exception {
        System.out.println("-- creating channel --");
        ch.set(createChannel(true));
        System.out.println("-- connecting channel --");
        final String GROUP=getUniqueClusterName("CloseTest.testConnectCloseSequenceWith2Members");
        ch.get().connect(GROUP);
        System.out.println("view is " + ch.get().getView());

        System.out.println("-- creating channel1 --");
        channel1.set(createChannel(ch.get()));
        System.out.println("-- connecting channel1 --");
        channel1.get().connect(GROUP);
        System.out.println("view is " + channel1.get().getView());

        System.out.println("-- closing channel1 --");
        channel1.get().close();

        Util.sleep(2000);
        System.out.println("-- closing channel --");
        ch.get().close();
    }


    public void testCreationAndClose2() throws Exception {
        System.out.println("-- creating channel2 --");
        ch.set(createChannel(true));
        System.out.println("-- connecting channel2 --");
        ch.get().connect(getUniqueClusterName("CloseTest.testCreationAndClose2"));
        System.out.println("-- closing channel --");
        ch.get().close();
    }


    public void testClosedChannel() throws Exception {
        System.out.println("-- creating channel --");
        ch.set(createChannel(true));
        System.out.println("-- connecting channel --");
        ch.get().connect(getUniqueClusterName("CloseTest.testClosedChannel"));
        System.out.println("-- closing channel --");
        ch.get().close();
        Util.sleep(2000);

        try {
            ch.get().connect(getUniqueClusterName("CloseTest.testClosedChannel"));
            assert false;
        }
        catch(IllegalStateException ex) {
            assertTrue(true);
        }
    }

   

    public void testMultipleConnectsAndDisconnects() throws Exception {
        c1.set(createChannel(true));
        assertTrue(c1.get().isOpen());
        assertFalse(c1.get().isConnected());
        final String GROUP=getUniqueClusterName("CloseTest.testMultipleConnectsAndDisconnects");
        c1.get().connect(GROUP);
        System.out.println("view after c1.connect(): " + c1.get().getView());
        assertTrue(c1.get().isOpen());
        assertTrue(c1.get().isConnected());
        assertServiceAndClusterView(c1.get(), 1);

        c2.set(createChannel(c1.get()));
        assertTrue(c2.get().isOpen());
        assertFalse(c2.get().isConnected());

        c2.get().connect(GROUP);
        System.out.println("view after c2.connect(): " + c2.get().getView());
        assertTrue(c2.get().isOpen());
        assertTrue(c2.get().isConnected());
        assertServiceAndClusterView(c2.get(), 2);
        Util.sleep(500);
        assertServiceAndClusterView(c1.get(), 2);

        c2.get().disconnect();
        System.out.println("view after c2.disconnect(): " + c2.get().getView());
        assertTrue(c2.get().isOpen());
        assertFalse(c2.get().isConnected());
        Util.sleep(500);
        assertServiceAndClusterView(c1.get(), 1);

        c2.get().connect(GROUP);
        System.out.println("view after c2.connect(): " + c2.get().getView());
        assertTrue(c2.get().isOpen());
        assertTrue(c2.get().isConnected());
        assertServiceAndClusterView(c2.get(), 2);
        Util.sleep(300);
        assertServiceAndClusterView(c1.get(), 2);

        // Now see what happens if we reconnect the first channel
        c3.set(createChannel(c1.get()));
        assertTrue(c3.get().isOpen());
        assertFalse(c3.get().isConnected());
        assertServiceAndClusterView(c1.get(), 2);
        assertServiceAndClusterView(c2.get(), 2);

        c1.get().disconnect();
        Util.sleep(1000);
        assertTrue(c1.get().isOpen());
        assertFalse(c1.get().isConnected());
        assertServiceAndClusterView(c2.get(), 1);
        assertTrue(c3.get().isOpen());
        assertFalse(c3.get().isConnected());

        c1.get().connect(GROUP);
        System.out.println("view after c1.connect(): " + c1.get().getView());
        assertTrue(c1.get().isOpen());
        assertTrue(c1.get().isConnected());
        assertServiceAndClusterView(c1.get(), 2);
        Util.sleep(500);
        assertServiceAndClusterView(c2.get(), 2);
        assertTrue(c3.get().isOpen());
        assertFalse(c3.get().isConnected());
    }


    private static void assertServiceAndClusterView(Channel ch, int num) {
        View view=ch.getView();
        String msg="view=" + view;
        assertNotNull(view);
        Assert.assertEquals(view.size(), num, msg);
    }


    private static class MyReceiver extends ReceiverAdapter {
        final List<View> views=new ArrayList<View>();
        public void viewAccepted(View new_view) {
            views.add(new_view);
            System.out.println("new_view = " + new_view);
        }
        public List<View> getViews() {return views;}
        public void clearViews() {views.clear();}
    }

}


package org.jgroups;


import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.logging.Log;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.SocketFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * A channel represents a group communication endpoint (like BSD datagram sockets). A client joins a
 * group by connecting the channel to a group and leaves it by disconnecting. Messages sent over the
 * channel are received by all group members that are connected to the same group (that is, all
 * members that have the same group name).
 * <p/>
 * 
 * The FSM for a channel is roughly as follows: a channel is created (<em>unconnected</em>). The
 * channel is connected to a group (<em>connected</em>). Messages can now be sent and received. The
 * channel is disconnected from the group (<em>unconnected</em>). The channel could now be connected
 * to a different group again. The channel is closed (<em>closed</em>).
 * <p/>
 * 
 * Only a single sender is allowed to be connected to a channel at a time, but there can be more
 * than one channel in an application.
 * <p/>
 * 
 * Messages can be sent to the group members using the <em>send</em> method and messages can be
 * received setting a {@link Receiver} in {@link #setReceiver(Receiver)} and implementing the
 * {@link Receiver#receive(Message)} callback.
 * <p>
 * 
 * A channel instance is created using the public constructor.
 * <p/>
 * Various degrees of sophistication in message exchange can be achieved using building blocks on
 * top of channels; e.g., light-weight groups, synchronous message invocation, or remote method
 * calls. Channels are on the same abstraction level as sockets, and should really be simple to use.
 * Higher-level abstractions are all built on top of channels.
 * 
 * @author Bela Ban
 * @since 2.0
 * @see java.net.DatagramPacket
 * @see java.net.MulticastSocket
 * @see JChannel
 */
@MBean(description="Channel")
public abstract class Channel /* implements Transport */ {
    protected UpHandler            up_handler=null;   // when set, <em>all</em> events are passed to it !
    protected Set<ChannelListener> channel_listeners=null;
    protected Receiver             receiver=null;
    protected SocketFactory        socket_factory=new DefaultSocketFactory();

    @ManagedAttribute(description="Whether or not to discard messages sent by this channel",writable=true)
    protected boolean discard_own_messages=false;

    protected abstract Log getLog();


    public abstract ProtocolStack getProtocolStack();

    public SocketFactory getSocketFactory() {
        return socket_factory;
    }

    public void setSocketFactory(SocketFactory factory) {
        socket_factory=factory;
        ProtocolStack stack=getProtocolStack();
        Protocol prot=stack != null? stack.getTopProtocol() : null;
        if(prot != null)
            prot.setSocketFactory(factory);
    }

   /**
    * Connects the channel to a group. The client is now able to receive group messages, views and
    * to send messages to (all or single) group members. This is a null operation if already
    * connected.
    * <p>
    * 
    * All channels with the same name form a group, that means all messages sent to the group will
    * be received by all channels connected to the same cluster name.
    * <p>
    * 
    * @param cluster_name
    *           The name of the channel to connect to.
    * @exception Exception
    *               The protocol stack cannot be started
    * @exception IllegalStateException
    *               The channel is closed
    */
    abstract public void connect(String cluster_name) throws Exception;


    /**
     * Connects this channel to a group and gets a state from a specified state provider.
     * <p/>
     *
     * This method essentially invokes
     * <code>connect<code> and <code>getState<code> methods successively.
     * If FLUSH protocol is in channel's stack definition only one flush is executed for both connecting and
     * fetching state rather than two flushes if we invoke <code>connect<code> and <code>getState<code> in succession.
     *
     * If the channel is closed an exception will be thrown.
     *
     *
     * @param cluster_name  the cluster name to connect to. Cannot be null.
     * @param target the state provider. If null state will be fetched from coordinator, unless this channel is coordinator.
     * @param timeout the timeout for state transfer.
     *
     * @exception Exception Connecting to the cluster or state transfer was not successful
     * @exception IllegalStateException The channel is closed and therefore cannot be used
     *
     */
    abstract public void connect(String cluster_name, Address target, long timeout) throws Exception;


   /**
    * Disconnects the channel if it is connected. If the channel is closed or disconnected, this
    * operation is ignored<br/>
    * The channel can then be connected to the same or a different cluster again.
    * 
    * @see #connect(String)
    */
    abstract public void disconnect();


   /**
    * Destroys the channel and its associated resources (e.g., the protocol stack). After a channel
    * has been closed, invoking methods on it throws the <code>ChannelClosed</code> exception (or
    * results in a null operation). It is a null operation if the channel is already closed.
    * <p>
    * If the channel is connected to a group, <code>disconnect()</code> will be called first.
    */
    abstract public void close();



   /**
    * Determines whether the channel is open; ie. the protocol stack has been created (may not be
    * connected though).
    * 
    * @return true is channel is open, false otherwise
    */
    abstract public boolean isOpen();


   /**
    * Determines whether the channel is connected to a group.
    * 
    * @return true if channel is connected to cluster (group) and can send/receive messages, false otherwise 
    */
    abstract public boolean isConnected();



   /**
    * Returns a map of statistics of the various protocols and of the channel itself.
    * 
    * @return Map<String,Map>. A map where the keys are the protocols ("channel" pseudo key is used
    *         for the channel itself") and the values are property maps.
    */
    public abstract Map<String,Object> dumpStats();

   /**
    * Sends a message. The message contains
    * <ol>
    * <li>a destination address (Address). A <code>null</code> address sends the message to all
    * group members.
    * <li>a source address. Can be left empty as it will be assigned automatically
    * <li>a byte buffer. The message contents.
    * <li>several additional fields. They can be used by application programs (or patterns). E.g. a
    * message ID, flags etc
    * </ol>
    * 
    * @param msg
    *           The message to be sent. Destination and buffer should be set. A null destination
    *           means to send to all group members.
    * @exception IllegalStateException
    *               thrown if the channel is disconnected or closed
    */
    abstract public void send(Message msg) throws Exception;


   /**
    * Helper method to create a Message with given parameters and invoke {@link #send(Message)}.
    * 
    * @param dst
    *           Destination address for message. If null, message will be sent to all current group
    *           members
    * @param obj
    *           A serializable object. Will be marshalled into the byte buffer of the Message. If it
    *           is <em>not</em> serializable, an exception will be thrown
    * @throws Exception
    *            exception thrown if message sending was not successful
    */
    abstract public void send(Address dst, Object obj) throws Exception;

   /**
    * Sends a message. See {@link #send(Address,byte[],int,int)} for details
    * 
    * @param dst
    *           destination address for message. If null, message will be sent to all current group
    *           members
    * @param buf
    *           buffer message payload
    * @throws Exception
    *            exception thrown if message sending was not successful
    */
    abstract public void send(Address dst, byte[] buf) throws Exception;

   /**
    * Sends a message to a destination.
    * 
    * @param dst
    *           The destination address. If null, the message will be sent to all cluster nodes (=
    *           group members)
    * @param buf
    *           The buffer to be sent
    * @param offset
    *           The offset into the buffer
    * @param length
    *           The length of the data to be sent. Has to be <= buf.length - offset. This will send
    *           <code>length</code> bytes starting at <code>offset</code>
    * @throws Exception
    *            If send() failed
    */
    abstract public void send(Address dst, byte[] buf, int offset, int length) throws Exception;


   /**
    * Enables access to event mechanism of a channel and is normally not used by clients directly.
    * 
    * @param sends an Event to a specific protocol layer and receive a response. 
    * @return a response from a particular protocol layer targeted by Event parameter
    */
    public Object down(Event evt) {
        return null;
    }


   /**
    * Gets the current view. The view may only be available after a successful
    * <code>connect()</code>. The result of calling this method on an unconnected channel is
    * implementation defined (may return null). Calling this method on a closed channel returns a
    * null view.
    * 
    * @return The current view.
    */
    abstract public View getView();


   /**
    * Returns the channel's own address. The result of calling this method on an unconnected channel
    * is implementation defined (may return null). Calling this method on a closed channel returns
    * null. Addresses can be used as destination in the <code>send()</code> operation.
    * 
    * @return The channel's address (opaque)
    */
    abstract public Address getAddress();

   /**
    * Returns the logical name of this channel if set.
    * 
    * @return The logical name or null (if not set)
    */
    abstract public String getName();

   /**
    * Returns the logical name of a given member. The lookup is from the local cache of logical
    * address / logical name mappings and no remote communication is performed.
    * 
    * @param member
    * @return The logical name for <code>member</code>
    */
    abstract public String getName(Address member);


   /**
    * Sets the logical name for the channel. The name will stay associated with this channel for the
    * channel's lifetime (until close() is called). This method should be called <em>before</em>
    * calling connect().
    * 
    * @param name
    */
    abstract public void setName(String name);


   /**
    * Returns the cluster name of the group of which the channel is a member. This is the object
    * that was the argument to <code>connect()</code>. Calling this method on a closed channel
    * returns <code>null</code>.
    * 
    * @return The cluster name
    */
    abstract public String getClusterName();


    public String getProperties() {
        return "n/a";
    }

   /**
    * Sets this channel event handler to be a recipient off all events . These will not be received
    * by the channel (except connect/disconnect, state retrieval and the like). This can be used by
    * building blocks on top of a channel; thus the channel is used as a pass-through medium, and
    * the building blocks take over some of the channel's tasks. However, tasks such as connection
    * management and state transfer is still handled by the channel.
    * 
    * @param the
    *           handler to handle channel events
    */
    public void setUpHandler(UpHandler up_handler) {
        this.up_handler=up_handler;
    }

    /**
    * Returns UpHandler installed for this channel
    * 
    * @return the installed UpHandler implementation
    */
   public UpHandler getUpHandler() {
        return up_handler;
    }

   /**
    * Adds a ChannelListener instance that will be notified when a channel event such as connect,
    * disconnect or close occurs.
    * 
    * @param listener
    *           to be notified
    */
    public synchronized void addChannelListener(ChannelListener listener) {
        if(listener == null)
            return;
        if(channel_listeners == null)
            channel_listeners=new CopyOnWriteArraySet<ChannelListener>();
        channel_listeners.add(listener);
    }

    /**
    * Removes a ChannelListener previously installed
    * 
    * @param listener to be removed
    */
    public synchronized void removeChannelListener(ChannelListener listener) {
        if(channel_listeners != null && listener != null)
            channel_listeners.remove(listener);
    }

    /**
    * Clears all installed ChannelListener instances
    * 
    */
   public synchronized void clearChannelListeners() {
        if(channel_listeners != null)
            channel_listeners.clear();
    }

   /**
    * Sets the receiver for this channel. Receiver will in turn handle all messages, view changes,
    * implement state transfer logic and so on.
    * 
    * @param r the receiver instance for this channel
    * @see Receiver
    * 
    * */
    public void setReceiver(Receiver r) {
        receiver=r;
    }

   /**
    * Returns a receiver for this channel if it has been installed using
    * {@link Channel#setReceiver(Receiver)} , null otherwise
    * 
    * @return a receiver installed on this channel
    */
   public Receiver getReceiver() {
        return receiver;
    }

   /**
    * When set to true, all messages sent by a member A will be discarded by A.
    * 
    * @param flag
    */
    public void setDiscardOwnMessages(boolean flag) {discard_own_messages=flag;}
    
    /**
    * Returns true if this channel will discard its own messages, false otherwise
    * 
    * @return
    */
   public boolean getDiscardOwnMessages() {return discard_own_messages;}

    abstract public boolean flushSupported();

   /**
    * Performs a partial flush in a cluster for flush participants.
    * <p/>
    * All pending messages are flushed out only for the flush participants. The remaining members in
    * a cluster are not included in the flush. The flush participants should be a proper subset of a
    * current view.
    * <p/>
    * 
    * @param automatic_resume
    *           if true call {@link #stopFlush()} after the flush
    * @see #startFlush(boolean)
    */
    abstract public void startFlush(List<Address> flushParticipants, boolean automatic_resume)
                throws Exception;

   /**
    * Will perform a flush of the system, ie. all pending messages are flushed out of the system and
    * all members ack their reception. After this call returns, no member will be sending any
    * messages until {@link #stopFlush()} is called.
    * <p/>
    * In case of flush collisions, a random sleep time backoff algorithm is employed and the flush
    * is reattempted for numberOfAttempts. Therefore this method is guaranteed to return after
    * timeout x numberOfAttempts milliseconds.
    * 
    * @param automatic_resume
    *           if true call {@link #stopFlush()} after the flush
    */
    abstract public void startFlush(boolean automatic_resume) throws Exception;
    
    abstract public void stopFlush();
    
    abstract public void stopFlush(List<Address> flushParticipants);


   /**
    * Retrieves the full state from the target member.
    * <p>
    * State transfer is initiated by invoking getState on this channel. The state provider in turn
    * invokes {@link MessageListener#getState(java.io.OutputStream)} callback and sends a state to
    * this node, the state receiver. After the state arrives to the state receiver
    * {@link MessageListener#setState(java.io.InputStream)} callback is invoked to install the
    * state.
    * 
    * @param target
    *           The state provider. If null the coordinator is used by default
    * @param timeout
    *           The number of milliseconds to wait for the operation to complete successfully. 0
    *           waits until the state has been received
    * 
    * @see MessageListener#getState(java.io.OutputStream)
    * @see MessageListener#setState(java.io.InputStream)
    * 
    * @exception IllegalStateException
    *               The channel was closed or disconnected, or the flush (if present) failed
    * @exception StateTransferException
    *               raised if there was a problem during the state transfer
    */
    abstract public void getState(Address target, long timeout) throws Exception;




    protected void notifyChannelConnected(Channel c) {
        if(channel_listeners == null) return;
        for(ChannelListener channelListener: channel_listeners) {
            try {
                channelListener.channelConnected(c);
            }
            catch(Throwable t) {
                getLog().error("exception in channelConnected() callback", t);
            }
        }
    }

    protected void notifyChannelDisconnected(Channel c) {
        if(channel_listeners == null) return;
        for(ChannelListener channelListener: channel_listeners) {
            try {
                channelListener.channelDisconnected(c);
            }
            catch(Throwable t) {
                getLog().error("exception in channelDisonnected() callback", t);
            }
        }
    }

    protected void notifyChannelClosed(Channel c) {
        if(channel_listeners == null) return;
        for(ChannelListener channelListener: channel_listeners) {
            try {
                channelListener.channelClosed(c);
            }
            catch(Throwable t) {
                getLog().error("exception in channelClosed() callback", t);
            }
        }
    }

   

}

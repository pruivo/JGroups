package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * When the SEQUENCER is used, it blocks the deliver of the messages in the coordinator until f+1 nodes have acked the
 * Message.
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class ACK_SEQUENCER extends Protocol {

   @Property(description = "The number of failed members expected (f). Before deliver a message, it will wait for f+1 " +
         "Acks before deliver the message. This number is adjustable dynamic when the property percentageOfFailedMembers" +
         " is set", writable = true)
   private volatile int expectedNumberOfFailedMembers = 2;

   @Property(description = "The percentage of expected failed member. This will update dynamic the f value. -1 disables " +
         "it and only accepts values between 0.0 (no waiting for acks) and 1.0 (wait all acks)", writable = true)
   private volatile double percentageOfFailedMembers = -1; //0.1 to 1.0. -1 == disable

   private volatile int actualNumberOfMembers = 0;
   private short sequencerHeaderID = -1;
   private volatile boolean isCoordinator = false;
   private volatile Address coordinatorAddress = null;
   private volatile View actualView = null;
   private Address localAddress = null;

   private boolean trace = log.isTraceEnabled();

   private final Map<Address, MessageWindow> origMbrAckMap = new HashMap<Address, MessageWindow>();

   @ManagedOperation
   public void setExpectedNumberOfFailedMembers(int expectedNumberOfFailedMembers) {
      this.expectedNumberOfFailedMembers = expectedNumberOfFailedMembers;
   }

   @ManagedOperation
   public void setPercentageOfFailedMembers(double percentageOfFailedMembers) {
      if (percentageOfFailedMembers > 1.0 || percentageOfFailedMembers < -1.0) {
         throw new IllegalArgumentException("Percentage of failed members only accepts values between -1 and 100" +
                                                  " inclusive. value received is " + percentageOfFailedMembers);
      }
      this.percentageOfFailedMembers = percentageOfFailedMembers;
      updateExpectedNumberOfFailedMembers();
   }

   @Override
   public void start() throws Exception {
      Protocol down = getDownProtocol();

      while (down != null) {
         if (down.getClass() == SEQUENCER.class) {
            sequencerHeaderID = down.getId();
            break;
         }
         down = down.getDownProtocol();
      }

      log.warn("SEQUENCER not found in the protocol stack or it is above this protocol. ACK_SEQUENCER will be disabled");
   }

   @Override
   public Object up(Event evt) {
      switch(evt.getType()) {
         case Event.MSG:
            if (sequencerHeaderID == -1) {
               break;
            }
            Message message = (Message) evt.getArg();
            AckSequencerHeader ack = (AckSequencerHeader) message.getHeader(id);

            if (ack != null) {
               return handleAck(ack.getOriginalSender(), ack.getSeqNo(), message.getSrc());
            }

            SEQUENCER.SequencerHeader sequencerHeader = (SEQUENCER.SequencerHeader) message.getHeader(sequencerHeaderID);

            if (sequencerHeader != null) {
               return handleMessage(sequencerHeader.getOriginalSender(), sequencerHeader.getSeqno(), message);
            }

            break;
         case Event.VIEW_CHANGE:
            handleViewChange((View)evt.getArg());
            break;
         case Event.SET_LOCAL_ADDRESS:
            localAddress=(Address)evt.getArg();
            break;
      }

      return up_prot.up(evt);
   }

   @Override
   public Object down(Event evt) {
      switch(evt.getType()) {
         case Event.VIEW_CHANGE:
            handleViewChange((View)evt.getArg());
            break;
         case Event.SET_LOCAL_ADDRESS:
            localAddress=(Address)evt.getArg();
            break;
      }

      return down_prot.down(evt);
   }

   private Object handleMessage(Address originalSender, long seqNo, Message message) {
      if (isCoordinator) {

         if (trace) {
            log.trace("Try to delivering the message " + message + " in the coordinator. Checking for ACKs...");
         }

         MessageWindow messageWindow = getMessageWindows(originalSender);
         try {
            messageWindow.waitUntilDeliverIsPossible(seqNo, expectedNumberOfFailedMembers, actualView.getMembers(),
                                                     localAddress);
         } catch (InterruptedException e) {
            log.warn("Interrupted Exception received while waiting for the ACKs. Delivering message...");
         }

         if (trace) {
            log.trace("Delivering message " + message + " in the coordinator");
         }
      } else {
         Message ack = new Message(coordinatorAddress);
         ack.setSrc(localAddress);
         ack.putHeader(id, new AckSequencerHeader(originalSender, seqNo));
         ack.setFlag(Message.Flag.OOB, Message.Flag.NO_TOTAL_ORDER, Message.Flag.NO_FC);

         if (trace) {
            log.trace("Delivering the message " + message + " in a non-coordinator member. Sending ACK [" + ack +
                            "] to " + coordinatorAddress);
         }

         try {
            down_prot.down(new Event(Event.MSG, ack));
         } catch (Exception e) {
            log.warn("Exception caught while sending the ACK to coordinator. [" + ack + "]");
         }
      }
      return up_prot.up(new Event(Event.MSG, message));
   }

   private Object handleAck(Address originalSender, long seqNo, Address from) {
      if (trace) {
         log.trace("Received ACK from " + from + " corresponding to the message [" + originalSender + "," +
         seqNo + "]");
      }
      MessageWindow messageWindow = getMessageWindows(originalSender);
      messageWindow.addAck(from, seqNo, expectedNumberOfFailedMembers, actualView.getMembers());
      return null;
   }

   private void handleViewChange(View view) {
      synchronized (origMbrAckMap) {
         actualView = view;
         coordinatorAddress = view.getMembers().get(0);
         isCoordinator = coordinatorAddress.equals(localAddress);
         actualNumberOfMembers = view.getMembers().size();
         updateExpectedNumberOfFailedMembers();

         if (!isCoordinator) {
            origMbrAckMap.clear();
            return;
         }

         for (Address address : view.getMembers()) {
            if (!origMbrAckMap.containsKey(address)) {
               origMbrAckMap.put(address, new MessageWindow());
            }
         }

         if (trace) {
            log.trace("Handle view change. Coordinator is " + coordinatorAddress + " and the number of expected failed " +
                            "member is " + expectedNumberOfFailedMembers);
         }
      }
   }

   public MessageWindow getMessageWindows(Address address) {
      synchronized (origMbrAckMap) {
         return origMbrAckMap.get(address);
      }
   }

   private void updateExpectedNumberOfFailedMembers() {
      if (percentageOfFailedMembers < 0) {
         return;
      }
      expectedNumberOfFailedMembers = (int)(actualNumberOfMembers * percentageOfFailedMembers) + 1;
   }

   public static class AckSequencerHeader extends Header {
      private Address originalSender;
      private long seqNo;

      public AckSequencerHeader(Address originalSender, long seqNo) {
         this.originalSender = originalSender;
         this.seqNo = seqNo;
      }

      @Override
      public int size() {
         return Util.size(originalSender) + Util.size(seqNo);
      }

      @Override
      public void writeTo(DataOutput out) throws Exception {
         Util.writeAddress(originalSender, out);
         Util.writeLong(seqNo, out);
      }

      @Override
      public void readFrom(DataInput in) throws Exception {
         originalSender = Util.readAddress(in);
         seqNo = Util.readLong(in);
      }

      public Address getOriginalSender() {
         return originalSender;
      }

      public long getSeqNo() {
         return seqNo;
      }
   }

   public static class AckCollector {
      private Set<Address> membersMissing = null;
      private int numberOfAcksMissing = -1;

      public synchronized void await(int acksExpected, Collection<Address> members, Address localAddress) throws InterruptedException {
         populateIfNeeded(acksExpected, members);
         membersMissing.remove(localAddress);
         if (numberOfAcksMissing > 0 && !membersMissing.isEmpty()) {
            this.wait();
         }
         membersMissing.clear();
      }

      public synchronized void ack(Address from, int acksExpected, Collection<Address> members) {
         populateIfNeeded(acksExpected, members);
         if (membersMissing.remove(from)) {
            numberOfAcksMissing--;
         }
         if (numberOfAcksMissing == 0 || membersMissing.isEmpty()) {
            this.notify();
         }
      }

      //TEST_ONLY
      public synchronized void populateIfNeeded(int acksExpected, Collection<Address> members) {
         if (membersMissing == null) {
            this.numberOfAcksMissing = acksExpected;
            this.membersMissing = new HashSet<Address>(members);
         }
      }

      //TEST ONLY!!
      public int getNumberOfAcksMissing() {
         return numberOfAcksMissing;
      }

      //TEST ONLY!!
      public int getSizeOfMembersMissing() {
         return membersMissing.size();
      }

      @Override
      public String toString() {
         return "AckCollector{" +
               "membersMissing=" + membersMissing +
               ", numberOfAcksMissing=" + numberOfAcksMissing +
               '}';
      }
   }

   public static class MessageWindow {
      private volatile long highestDeliverMessageSeqNo = 0;
      private ConcurrentSkipListMap<Long, AckCollector> ackWindow = new ConcurrentSkipListMap<Long, AckCollector>();

      public void waitUntilDeliverIsPossible(long seqNo, int numberOfAcksMissing, Collection<Address> members,
                                             Address localAddress)
            throws InterruptedException {
         highestDeliverMessageSeqNo = seqNo;
         AckCollector ackCollector = getOrCreate(seqNo);
         ackCollector.await(numberOfAcksMissing, members, localAddress);
         removeOldAckCollectors();
      }

      public void addAck(Address from, long seqNo, int numberOfAcksMissing, Collection<Address> members) {
         AckCollector ackCollector = getOrCreate(seqNo);
         if (ackCollector == null || seqNo < highestDeliverMessageSeqNo) {
            removeOldAckCollectors();
            return;
         }
         ackCollector.ack(from, numberOfAcksMissing, members);
      }

      //test
      public AckCollector getOrCreate(long seqNo) {
         AckCollector ackCollector = ackWindow.get(seqNo);
         if (ackCollector == null && seqNo >= highestDeliverMessageSeqNo) {
            ackCollector =  new AckCollector();
            AckCollector existing = ackWindow.putIfAbsent(seqNo, ackCollector);
            if (existing != null) {
               ackCollector = existing;
            }
         }
         removeOldAckCollectors();
         return ackCollector;
      }

      private void removeOldAckCollectors() {
         do {
            if (ackWindow.isEmpty()) {
               return;
            }
            long key = ackWindow.firstKey();
            if (key < highestDeliverMessageSeqNo) {
               ackWindow.remove(key);
            } else {
               break;
            }
         } while (true);
      }

      //test
      public long getHighestDeliverMessageSeqNo() {
         return highestDeliverMessageSeqNo;
      }

      //test
      public ConcurrentSkipListMap<Long, AckCollector> getAckWindow() {
         return ackWindow;
      }

      @Override
      public String toString() {
         return "MessageWindow{" +
               "highestDeliverMessageSeqNo=" + highestDeliverMessageSeqNo +
               ", ackWindow=" + ackWindow +
               '}';
      }
   }

   @ManagedAttribute
   public int getExpectedNumberOfFailedMembers() {
      return expectedNumberOfFailedMembers;
   }

   @ManagedAttribute
   public double getPercentageOfFailedMembers() {
      return percentageOfFailedMembers * 100;
   }

   @ManagedAttribute
   public int getActualNumberOfMembers() {
      return actualNumberOfMembers;
   }

   @ManagedAttribute
   public boolean isCoordinator() {
      return isCoordinator;
   }

   @ManagedAttribute
   public Address getCoordinatorAddress() {
      return coordinatorAddress;
   }

   //TEST_ONLY
   public short getSequencerHeaderID() {
      return sequencerHeaderID;
   }
}

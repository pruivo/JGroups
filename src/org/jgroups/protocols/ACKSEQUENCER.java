package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * When the SEQUENCER is used, it blocks the deliver of the messages in the coordinator until f+1 nodes have acked the
 * reception of the Message.
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class ACKSEQUENCER extends Protocol {

   @Property(description = "The number of failed members expected (f). Before deliver a message, it will wait for f+1 " +
         "Acks before deliver the message. This number is adjustable dynamic when the property percentageOfFailedMembers" +
         " is set", writable = true)
   private volatile int expectedNumberOfFailedMembers = 2;

   @Property(description = "The percentage of expected failed member. This will update dynamic the f value. -1 disables " +
         "it and only accepts values between 0 (no waiting for acks) and 100 (wait all acks)", writable = true)
   private volatile int percentageOfFailedMembers = -1; //0 a 100. -1 == disable

   private volatile int actualNumberOfMembers = 0;
   private volatile short sequencerHeaderID = -1;
   private volatile boolean isCoordinator = false;
   private volatile Address coordinatorAddress = null;
   private Address localAddress = null;

   public void setExpectedNumberOfFailedMembers(int expectedNumberOfFailedMembers) {
      this.expectedNumberOfFailedMembers = expectedNumberOfFailedMembers;
   }

   public void setPercentageOfFailedMembers(int percentageOfFailedMembers) {
      if (percentageOfFailedMembers > 100 || percentageOfFailedMembers < -1) {
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

      log.warn("SEQUENCER not found in the protocol stack or it is above this protocol. ACKSEQUENCER will be disabled");
   }

   @Override
   public Object up(Event evt) {
      if (sequencerHeaderID == -1) {
         return up_prot.up(evt);
      }

      switch(evt.getType()) {
         case Event.MSG:
            Message message = (Message) evt.getArg();
            AckSequencerHeader ack = (AckSequencerHeader) message.getHeader(id);

            if (ack != null) {
               return handleAck(ack.getMessageTag(), message.getSrc());
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

   private Object handleMessage(Address originalSender, long seqno, Message message) {
      //TODO
      if (isCoordinator) {
         //block the message until all acks are received
      } else {
         Message ack = new Message(coordinatorAddress);
         ack.setSrc(localAddress);
         ack.putHeader(id, new AckSequencerHeader(originalSender, seqno));
         ack.setFlag(Message.Flag.OOB, Message.Flag.NO_TOTAL_ORDER, Message.Flag.NO_FC);

         try {
            down_prot.down(new Event(Event.MSG, ack));
         } catch (Exception e) {
            log.info("Exception caught when sending the ack");
         }
      }
      return up_prot.up(new Event(Event.MSG, message));
   }

   private Object handleAck(ViewId messageTag, Address src) {
      //TODO
      //just add ack
      return null;
   }


   @Override
   public Object down(Event evt) {
      if (sequencerHeaderID == -1) {
         return down_prot.down(evt);
      }

      switch(evt.getType()) {
         case Event.MSG:
            break;
         case Event.VIEW_CHANGE:
            handleViewChange((View)evt.getArg());
            break;

         case Event.SET_LOCAL_ADDRESS:
            localAddress=(Address)evt.getArg();
            break;
      }

      return down_prot.down(evt);
   }

   private void handleViewChange(View view) {
      coordinatorAddress = view.getMembers().get(0);
      isCoordinator = coordinatorAddress.equals(localAddress);
      actualNumberOfMembers = view.getMembers().size();
      updateExpectedNumberOfFailedMembers();
   }

   private void updateExpectedNumberOfFailedMembers() {
      if (percentageOfFailedMembers == -1) {
         return;
      }
      expectedNumberOfFailedMembers = actualNumberOfMembers * 100 / percentageOfFailedMembers;
   }

   private static class AckSequencerHeader extends Header {

      private ViewId messageTag;

      private AckSequencerHeader(Address originalSender, long seqNo) {
         this.messageTag = new ViewId(originalSender, seqNo);
      }

      public ViewId getMessageTag() {
         return messageTag;
      }

      @Override
      public int size() {
         return messageTag.serializedSize();
      }

      @Override
      public void writeTo(DataOutput out) throws Exception {
         Util.writeViewId(messageTag, out);
      }

      @Override
      public void readFrom(DataInput in) throws Exception {
         messageTag = Util.readViewId(in);
      }
   }
}

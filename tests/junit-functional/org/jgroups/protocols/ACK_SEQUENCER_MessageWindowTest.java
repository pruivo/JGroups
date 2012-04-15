package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Set;

/**
 * Test battery to the behavior of the MessageWindow structure in ACK_SEQUENCER
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
@Test(groups = Global.FUNCTIONAL, sequential = true)

public class ACK_SEQUENCER_MessageWindowTest {

   public void testAddAck() {
      ACK_SEQUENCER.MessageWindow messageWindow = new ACK_SEQUENCER.MessageWindow();
      addAck(messageWindow, 0);
      check(messageWindow, 0, 0);
      addAck(messageWindow, 1);
      check(messageWindow, 0, 1);
      addAck(messageWindow, 3);
      checkSequence(messageWindow, 0, 1, 3);
      checkHighestSequenceNumber(messageWindow, 0);
   }

   public void testCleanDeliverMessage() {
      ACK_SEQUENCER.MessageWindow messageWindow = new ACK_SEQUENCER.MessageWindow();
      init(messageWindow, 0, 10);
      deliver(messageWindow, 10);
      check(messageWindow, 10, 10);
      checkHighestSequenceNumber(messageWindow, 10);
   }

   public void testAddOldAck() {
      ACK_SEQUENCER.MessageWindow messageWindow = new ACK_SEQUENCER.MessageWindow();
      init(messageWindow, 0, 10);
      deliver(messageWindow, 5);
      checkHighestSequenceNumber(messageWindow, 5);
      check(messageWindow, 5, 10);
      addAck(messageWindow, 3);
      checkHighestSequenceNumber(messageWindow, 5);
      check(messageWindow, 5, 10);
      addAck(messageWindow, 5);
      checkHighestSequenceNumber(messageWindow, 5);
      check(messageWindow, 5, 10);
   }

   public void testTryDeliver() {
      ACK_SEQUENCER.MessageWindow messageWindow = new ACK_SEQUENCER.MessageWindow();
      deliver(messageWindow, 10);
      check(messageWindow, 10, 10);
      checkHighestSequenceNumber(messageWindow, 10);
   }

   public void testCreateAckCollector() {
      ACK_SEQUENCER.MessageWindow messageWindow = new ACK_SEQUENCER.MessageWindow();
      init(messageWindow, 0, 10);
      deliver(messageWindow, 5);
      checkHighestSequenceNumber(messageWindow, 5);
      Object obj = messageWindow.getOrCreate(5);
      assert obj != null : "Expected ack collector for sequence number 5, but obtained null";
      obj = messageWindow.getOrCreate(4);
      assert obj == null : "Expected null ack collector but get the ack collector for sequence number 4";
      obj = messageWindow.getOrCreate(11);
      assert obj != null : "Expected ack collector for sequence number 11, but obtained null";
      Object obj2 = messageWindow.getOrCreate(11);
      assert obj == obj2 : "Expected the same ack collector for sequence number 11, but obtained a different object";
   }

   private void init(ACK_SEQUENCER.MessageWindow messageWindow, int begin, int end) {
      for (int i = begin; i <= end; ++i) {
         addAck(messageWindow, i);
      }
      checkHighestSequenceNumber(messageWindow, 0);
      check(messageWindow, begin, end);
   }

   private void checkHighestSequenceNumber(ACK_SEQUENCER.MessageWindow messageWindow, long expectedSeqNo) {
      assert messageWindow.getHighestDeliverMessageSeqNo() == expectedSeqNo : "Wrong highest sequence number. " +
            expectedSeqNo + "!=" + messageWindow.getHighestDeliverMessageSeqNo();
   }

   private void check(ACK_SEQUENCER.MessageWindow messageWindow, long begin, long end) {
      check(messageWindow.getAckWindow().keySet(), begin, end);
   }
   
   private void check(Set<Long> seqNos, long begin, long end) {
      assert seqNos.size() == end - begin + 1 : "Wrong sequence size";
      long value = begin;
      for (long seqNo : seqNos) {
         assert seqNo == value : "Wrong value in ack window." + seqNo + "!=" + value;
         value++;
      }
   }

   private void checkSequence(ACK_SEQUENCER.MessageWindow messageWindow, long... values) {
      Set<Long> longs = messageWindow.getAckWindow().keySet();
      assert longs.size() == values.length : "Wrong sequence size";
      
      int i = 0;
      for (long l : longs) {
         assert l == values[i] : "Wrong value in ack window." + l + "!=" + values[i];
         i++;
      }
   }

   private void deliver(ACK_SEQUENCER.MessageWindow messageWindow, long seqNo) {
      try {
         messageWindow.waitUntilDeliverIsPossible(seqNo, 0, Collections.<Address>emptyList(), Util.createRandomAddress());
      } catch (InterruptedException e) {
         assert false : "Interrupted Exception not expected!";
      }
   }

   private void addAck(ACK_SEQUENCER.MessageWindow messageWindow, long seqNo) {
      messageWindow.addAck(Util.createRandomAddress("A"), seqNo, 0, Collections.<Address>emptyList());
   }
}

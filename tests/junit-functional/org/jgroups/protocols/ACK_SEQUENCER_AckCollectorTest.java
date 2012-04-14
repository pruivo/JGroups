package jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.ACK_SEQUENCER;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test battery to the behavior of the AckCollector structure in ACK_SEQUENCER
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
@Test(groups = Global.FUNCTIONAL, sequential = true)
public class ACK_SEQUENCER_AckCollectorTest {

   public void testNonBlockingWaitDueToEmptyList() {
      Timer timer = new Timer();
      timer.schedule(cancelTask(), 10000);

      ACK_SEQUENCER.AckCollector ackCollector = new ACK_SEQUENCER.AckCollector();
      try {
         ackCollector.await(10, Collections.<Address>emptyList());
      } catch (InterruptedException e) {
         assert false : "blocked when try to await(10, emptyList)";
      }
      timer.cancel();
   }

   public void testNonBlockingWaitDueToZeroAcksNeeded() {
      List<Address> addressList = addressList();

      Timer timer = new Timer();
      timer.schedule(cancelTask(), 10000);

      ACK_SEQUENCER.AckCollector ackCollector = new ACK_SEQUENCER.AckCollector();
      try {
         ackCollector.await(0, addressList);
      } catch (InterruptedException e) {
         assert false : "blocked when try to await(0, nonEmptyList)";
      }
      timer.cancel();
   }

   public void testNonBlockingWaitDueToNegativeAcksNeeded() {
      List<Address> addressList = addressList();

      Timer timer = new Timer();
      timer.schedule(cancelTask(), 10000);

      ACK_SEQUENCER.AckCollector ackCollector = new ACK_SEQUENCER.AckCollector();
      try {
         ackCollector.await(-1, addressList);
      } catch (InterruptedException e) {
         assert false : "blocked when try to await(-1, nonEmptyList)";
      }
      timer.cancel();
   }

   public void testSameNumberOfAcksAndMembers() {
      List<Address> addressList = addressList();

      ACK_SEQUENCER.AckCollector ackCollector = new ACK_SEQUENCER.AckCollector();
      ackCollector.populateIfNeeded(addressList.size(), addressList);

      check(ackCollector, addressList.size(), addressList.size());

      CountDownLatch countDownLatch = new CountDownLatch(1);

      Thread thread = createNewAwaitThread(ackCollector, countDownLatch);
      thread.start();

      for (Address address : addressList) {
         checkBlocked(countDownLatch, ackCollector);
         ackCollector.ack(address, 0, Collections.<Address>emptyList());
      }

      checkUnblocked(countDownLatch, ackCollector);
      check(ackCollector, 0, 0);
   }

   public void testHigherNumberOfAcksNeededThanMemberList() {
      List<Address> addressList = addressList();

      ACK_SEQUENCER.AckCollector ackCollector = new ACK_SEQUENCER.AckCollector();
      ackCollector.populateIfNeeded(50, addressList);

      check(ackCollector, 50, addressList.size());

      CountDownLatch countDownLatch = new CountDownLatch(1);

      Thread thread = createNewAwaitThread(ackCollector, countDownLatch);
      thread.start();

      for (Address address : addressList) {
         checkBlocked(countDownLatch, ackCollector);
         ackCollector.ack(address, 0, Collections.<Address>emptyList());
      }

      checkUnblocked(countDownLatch, ackCollector);
      check(ackCollector, 40, 0);
   }

   public void testLowerAcksNeededThanMemberList() {
      List<Address> addressList = addressList();

      ACK_SEQUENCER.AckCollector ackCollector = new ACK_SEQUENCER.AckCollector();
      ackCollector.populateIfNeeded(2, addressList);

      check(ackCollector, 2, addressList.size());

      CountDownLatch countDownLatch = new CountDownLatch(1);

      Thread thread = createNewAwaitThread(ackCollector, countDownLatch);
      thread.start();

      checkBlocked(countDownLatch, ackCollector);
      ackCollector.ack(addressList.get(0), 0, Collections.<Address>emptyList());
      checkBlocked(countDownLatch, ackCollector);
      ackCollector.ack(addressList.get(1), 0, Collections.<Address>emptyList());

      checkUnblocked(countDownLatch, ackCollector);
      //its clear the member list after unblocking
      check(ackCollector, 0, 0);
   }

   public void testNonMemberAck() {
      List<Address> addressList = addressList();

      ACK_SEQUENCER.AckCollector ackCollector = new ACK_SEQUENCER.AckCollector();
      ackCollector.populateIfNeeded(addressList.size(), addressList);

      check(ackCollector, addressList.size(), addressList.size());

      ackCollector.ack(new TestAddress(-1), 0, Collections.<Address>emptyList());

      check(ackCollector, addressList.size(), addressList.size());
   }

   public void testDuplicatedAcks() {
      List<Address> addressList = addressList();

      ACK_SEQUENCER.AckCollector ackCollector = new ACK_SEQUENCER.AckCollector();
      ackCollector.populateIfNeeded(addressList.size(), addressList);

      check(ackCollector, addressList.size(), addressList.size());

      ackCollector.ack(addressList.get(0), 0, Collections.<Address>emptyList());
      ackCollector.ack(addressList.get(0), 0, Collections.<Address>emptyList());
      ackCollector.ack(addressList.get(0), 0, Collections.<Address>emptyList());

      check(ackCollector, addressList.size() - 1, addressList.size() - 1);
   }

   private void check(ACK_SEQUENCER.AckCollector ackCollector, int expectedAcks, int expectedSize) {
      assert expectedAcks == ackCollector.getNumberOfAcksMissing() : "Wrong state detected. Expected acks missing is " +
            expectedAcks + " but the collector has " + ackCollector.getNumberOfAcksMissing();
      assert expectedSize == ackCollector.getSizeOfMembersMissing() : "Wrong state detected. Expected member size is " +
            expectedSize + " but the collector has " + ackCollector.getSizeOfMembersMissing();
   }

   private void checkBlocked(CountDownLatch countDownLatch, ACK_SEQUENCER.AckCollector ackCollector) {
      assert countDownLatch.getCount() == 1 : "Thread was already unblocked but acks is still needed. State is " + ackCollector;
   }

   private void checkUnblocked(CountDownLatch countDownLatch, ACK_SEQUENCER.AckCollector ackCollector) {
      try {
         countDownLatch.await(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         assert false : "Thread is still blocked but *no* acks is needed. State is " + ackCollector;
      }
   }

   private TimerTask cancelTask() {
      final Thread workingThread = Thread.currentThread();
      return new TimerTask() {
         @Override
         public void run() {
            workingThread.interrupt();
         }
      };
   }

   private List<Address> addressList() {
      List<Address> addressList = new LinkedList<Address>();
      for (int i = 0; i < 10; i++) {
         addressList.add(new TestAddress(i));
      }
      return addressList;
   }

   private Thread createNewAwaitThread(final ACK_SEQUENCER.AckCollector ackCollector, final CountDownLatch countDownLatch) {
      return new Thread() {
         @Override
         public void run() {
            try {
               ackCollector.await(0, Collections.<Address>emptyList());
               countDownLatch.countDown();
            } catch (InterruptedException e) {/*just ignore*/}
         }
      };
   }

   private class TestAddress implements Address {
      int id;

      public TestAddress(int id) {
         this.id = id;
      }

      @SuppressWarnings("UnusedDeclaration")
      public TestAddress() {}

      @Override
      public int size() {
         return id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         TestAddress that = (TestAddress) o;

         return id == that.id;

      }

      @Override
      public int hashCode() {
         return id;
      }

      @Override
      public int compareTo(Address o) {
         return Integer.signum(id - o.size());
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {/*no-op*/}

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {/*no-op*/}

      @Override
      public void writeTo(DataOutput out) throws Exception {/*no-op*/}

      @Override
      public void readFrom(DataInput in) throws Exception {/*no-op*/}

      @Override
      public String toString() {
         return "TestAddress{" +
               "id=" + id +
               '}';
      }
   }
}

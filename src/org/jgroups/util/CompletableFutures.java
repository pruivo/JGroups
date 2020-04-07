package org.jgroups.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public final class CompletableFutures {

   private static final CompletableFuture<?> NULL = CompletableFuture.completedFuture(null);
   private static final Consumer<?> VOID_CONSUMER = o -> {
   };
   private static final Future<?> EMPTY_FUTURE = new EmptyFuture<>();

   private CompletableFutures() {
   }

   public static <T> T join(CompletableFuture<T> cf) {
      try {
         return cf.join();
      } catch (CompletionException e) {
         throw new RuntimeException(e.getCause());
      }
   }

   public static <T> CompletableFuture<T> completedNull() {
      //noinspection unchecked
      return (CompletableFuture<T>) NULL;
   }

   public static <T> Consumer<T> voidConsumer() {
      //noinspection unchecked
      return (Consumer<T>) VOID_CONSUMER;
   }

   public static <T> Future<T> nullFuture() {
      //noinspection unchecked
      return (Future<T>) EMPTY_FUTURE;
   }

   private static final class EmptyFuture<V> implements Future<V> {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
         return false;
      }

      @Override
      public boolean isCancelled() {
         return false;
      }

      @Override
      public boolean isDone() {
         return true;
      }

      @Override
      public V get() {
         return null;
      }

      @Override
      public V get(long timeout, TimeUnit unit) {
         return null;
      }
   }

}

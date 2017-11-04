/**
 * Copyright 2017 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.server.grpc;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RequestTrackerImplTest {

  @Test
  public void testCompleteWithZeroCount() throws Exception {
    final RequestTracker rt = new RequestTrackerImpl();
    assertComplete(rt.complete());
  }

  @Test
  public void testCompleteWaitForDecrement() throws Exception {
    final RequestTracker rt = new RequestTrackerImpl();
    rt.increment();
    final CompletableFuture<Void> future = rt.complete();
    assertFalse(future.isDone());
    rt.decrement();
    assertComplete(future);
  }

  @Test(expected = IllegalStateException.class)
  public void testDecrementBelowZero() throws Exception {
    final RequestTracker rt = new RequestTrackerImpl();
    rt.decrement();
  }

  @Test(expected = IllegalStateException.class)
  public void testDecrementBelowZeroAfterComplete() throws Exception {
    final RequestTracker rt = new RequestTrackerImpl();
    rt.increment();
    final CompletableFuture<Void> future = rt.complete();
    try {
      rt.decrement(2);
    } catch (IllegalStateException e) {
      assertTrue(future.isCompletedExceptionally());
      throw e;
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testDecrementBelowZeroBeforeComplete() throws Exception {
    final RequestTracker rt = new RequestTrackerImpl();
    try {
      rt.decrement();
    } catch (IllegalStateException e) {
      final CompletableFuture<Void> future = rt.complete();
      assertTrue(future.isCompletedExceptionally());
      throw e;
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testIncrementCompleted() throws Exception {
    final RequestTracker rt = new RequestTrackerImpl();
    rt.complete();
    rt.increment();
  }

  @Test(expected = IllegalStateException.class)
  public void testDecrementCompleted() throws Exception {
    final RequestTracker rt = new RequestTrackerImpl();
    rt.complete();
    rt.decrement();
  }

  @Test
  public void testCompleteAfterDecrementCustomCount() throws Exception {
    final RequestTracker rt = new RequestTrackerImpl();
    rt.increment(20);
    final CompletableFuture<Void> future = rt.complete();
    rt.decrement(15);
    rt.decrement(5);
    assertComplete(future);
  }

  private void assertComplete(CompletableFuture<Void> future) throws Exception {
    future.get(1000, TimeUnit.MILLISECONDS);
    assertTrue(future.isDone());
  }
}

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

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
public class RequestTrackerImpl implements RequestTracker {

  private final Object lock = new Object();

  private int activeCount = 0;
  private CompletableFuture<Void> completeFuture;

  @Override
  public void increment(int num) {
    synchronized (lock) {
      checkCompleted();
      // ~ sanity check
      if (completeFuture != null) {
        completeFuture.completeExceptionally(
            new IllegalStateException("Can not increment after the complete call."));
      }
      activeCount += num;
    }
  }

  @Override
  public void decrement(int num) {
    synchronized (lock) {
      checkCompleted();
      // ~ sanity check
      if (activeCount - num < 0) {
        final RuntimeException e =
            new IllegalStateException("Can not increment after the complete call.");
        if (completeFuture == null) {
          completeFuture = new CompletableFuture<>();
        }
        completeFuture.completeExceptionally(e);
        throw e;
      }
      activeCount -= num;
      // ~ complete if activeCount reaches zero
      if (activeCount == 0) {
        completeFuture.complete(null);
      }
    }
  }

  @Override
  public CompletableFuture<Void> complete() {
    synchronized (lock) {
      if (completeFuture == null) {
        completeFuture = new CompletableFuture<>();
        if (activeCount == 0) {
          completeFuture.complete(null);
        }
      }
      return completeFuture;
    }
  }

  /**
   * Throws an exception if user is trying to amend tracker that is already
   * completed.
   */
  private void checkCompleted() {
    synchronized (lock) {
      if (completeFuture != null && completeFuture.isDone()) {
        throw new IllegalStateException("Request tracker already completed.");
      }
    }
  }
}

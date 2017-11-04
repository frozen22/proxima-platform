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

package cz.o2.proxima.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Various utilities related to concurrency, threads etc.
 */
public class ExecutorUtils {

  /**
   * Returns {@link ThreadFactory} that creates daemon threads with given prefix.
   */
  public static ThreadFactory daemonThreadFactory(String threadPrefix) {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(threadPrefix + "-%d")
        .build();
  }

  /**
   * Gracefully closes executor service within given timeout.
   */
  public static void close(ExecutorService executorService, Duration timeout) {
    close(executorService, timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Gracefully closes executor service within given timeout.
   */
  public static void close(ExecutorService executorService, long timeout, TimeUnit unit) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(timeout, unit)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}


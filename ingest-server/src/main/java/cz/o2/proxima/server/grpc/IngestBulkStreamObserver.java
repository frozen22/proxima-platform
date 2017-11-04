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

import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.util.ExecutorUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class IngestBulkStreamObserver implements StreamObserver<Rpc.IngestBulk> {

  private static final long SHUTDOWN_TIMEOUT_MS = 5000;
  private static final long FLUSH_RATE_MS = 5000;
  private static final int STATUS_QUEUE_CAPACITY = 500;
  private static final int STATUS_BULK_SIZE = 1000;

  /**
   * Queue for processing statuses.
   */
  private final Queue<Rpc.Status> statusQueue = new ConcurrentLinkedQueue<>();

  /**
   * Timestamp when the last statuses were send to the client.
   */
  private final AtomicLong lastSend = new AtomicLong(System.currentTimeMillis());

  /**
   * Current bulk of unsent statuses.
   */
  private final Rpc.StatusBulk.Builder bulk = Rpc.StatusBulk.newBuilder();

  /**
   * Tracks in-flight requests, that needs to be persisted.
   */
  private final RequestTracker requestTracker = new RequestTrackerImpl();

  /**
   * Task that periodically flushes statuses to the client.
   */
  private final ScheduledFuture<?> flushFuture;

  /**
   * Observer for bulk of statuses.
   */
  private final StreamObserver<Rpc.StatusBulk> responseObserver;

  /**
   * Processes single ingest request.
   */
  private final RequestProcessor requestProcessor;

  /**
   * Executor service for async callbacks.
   */
  private final ExecutorService callbackExecutor;

  /**
   * Worker thread for all requests to the client.
   */
  private final ScheduledExecutorService executorService = Executors
      .newSingleThreadScheduledExecutor(ExecutorUtils.daemonThreadFactory("ingest-bulk"));

  private final Object flushLock = new Object();

  IngestBulkStreamObserver(StreamObserver<Rpc.StatusBulk> responseObserver,
                           RequestProcessor requestProcessor,
                           ExecutorService callbackExecutor) {
    this.responseObserver = responseObserver;
    this.requestProcessor = requestProcessor;
    this.callbackExecutor = callbackExecutor;
    // ~ schedule flush periodically
    this.flushFuture = executorService.scheduleAtFixedRate(
        this::flush, FLUSH_RATE_MS, FLUSH_RATE_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void onNext(Rpc.IngestBulk bulk) {
    Metrics.INGEST_BULK.increment();
    Metrics.BULK_SIZE.increment(bulk.getIngestCount());
    log.debug("Received a new bulk of {} items.", bulk.getIngestCount());
    requestTracker.increment(bulk.getIngestCount());
    bulk.getIngestList().forEach(this::processIngestRequest);
  }

  @Override
  public void onError(Throwable error) {
    log.error("Client side channel error.", error);
    flushFuture.cancel(true);
    ExecutorUtils.close(executorService, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void onCompleted() {
    flushFuture.cancel(false);

    // ~ wait for pending requests to complete
    requestTracker.complete().whenCompleteAsync((ignore, error) -> {
      while (fillBulk()) {
        sendBulk();
      }
      sendBulk();
      responseObserver.onCompleted();
    }, callbackExecutor);

    ExecutorUtils.close(executorService, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private void processIngestRequest(Rpc.Ingest ingest) {
    requestProcessor.process(ingest).whenCompleteAsync((status, error) -> {
      if (error != null) {
        log.error("Error processing the request.", error);
      }
      statusQueue.add(status);
      if (statusQueue.size() >= STATUS_QUEUE_CAPACITY) {
        flush();
      }
      requestTracker.decrement();
    }, callbackExecutor);
  }

  /**
   * Check for unsent statuses, bulk them and sent to client.
   */
  private void flush() {
    synchronized (flushLock) {
      while (fillBulk()) {
        sendBulk();
      }
      final long now = System.nanoTime();
      // ~ send after flush rate, even though bulk is not full
      if (now - lastSend.get() >= FLUSH_RATE_MS) {
        sendBulk();
      }
    }
  }

  /**
   * Add to new statuses to the current bulk.
   */
  private boolean fillBulk() {
    synchronized (flushLock) {
      Rpc.Status status;
      while ((status = statusQueue.poll()) != null) {
        bulk.addStatus(status);
        if (bulk.getStatusCount() >= STATUS_BULK_SIZE) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Send current bulk of statuses to the observer.
   */
  private void sendBulk() {
    synchronized (flushLock) {
      lastSend.set(System.currentTimeMillis());
      final Rpc.StatusBulk bulk = this.bulk.build();
      if (bulk.getStatusCount() > 0) {
        responseObserver.onNext(bulk);
      }
      this.bulk.clear();
    }
  }

}

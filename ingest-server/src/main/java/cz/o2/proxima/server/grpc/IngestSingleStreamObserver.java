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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;

@Slf4j
class IngestSingleStreamObserver implements StreamObserver<Rpc.Ingest> {

  private final RequestTracker requestTracker = new RequestTrackerImpl();

  private final StreamObserver<Rpc.Status> responseObserver;
  private final RequestProcessor requestProcessor;
  private final ExecutorService callbackExecutor;

  private final Object observerLock = new Object();

  IngestSingleStreamObserver(StreamObserver<Rpc.Status> responseObserver,
                             RequestProcessor requestProcessor,
                             ExecutorService callbackExecutor) {
    this.responseObserver = responseObserver;
    this.requestProcessor = requestProcessor;
    this.callbackExecutor = callbackExecutor;
  }

  @Override
  public void onNext(Rpc.Ingest request) {
    Metrics.INGEST_SINGLE.increment();
    // ~ track new in-flight request
    requestTracker.increment();
    // ~ let processor do the heavy lifting asynchronously
    requestProcessor.process(request).whenCompleteAsync((status, error) -> {
      synchronized (observerLock) {
        if (error != null) {
          responseObserver.onError(Status.INTERNAL
              .withCause(error)
              .asException());
        } else {
          responseObserver.onNext(status);
          // ~ mark this request as completed
          requestTracker.decrement();
        }
      }
    }, callbackExecutor);
  }

  @Override
  public void onError(Throwable error) {
    log.error("Client side channel error.", error);
  }

  @Override
  public void onCompleted() {
    requestTracker.complete().whenCompleteAsync((ignore, error) -> {
      synchronized (observerLock) {
        if (error != null) {
          log.error("Request tracker just got mad.", error);
          responseObserver.onError(Status.INTERNAL
              .withCause(error)
              .asException());
        } else {
          responseObserver.onCompleted();
        }
      }
    }, callbackExecutor);
  }
}

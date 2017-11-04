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
import cz.o2.proxima.server.grpc.utils.TestStreamObserver;
import cz.o2.proxima.util.ExecutorUtils;
import io.grpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class IngestSingleStreamObserverTest {

  private TestStreamObserver.Result<Rpc.Status> result;
  private RequestProcessor requestProcessor;
  private ExecutorService callbackExecutor;
  private IngestSingleStreamObserver observer;

  @Before
  public void setUp() {
    result = new TestStreamObserver.Result<>();
    requestProcessor = mock(RequestProcessor.class);
    callbackExecutor = Executors.newSingleThreadExecutor(
        ExecutorUtils.daemonThreadFactory("test-callback-executor"));
    observer = new IngestSingleStreamObserver(
        new TestStreamObserver<>(result), requestProcessor, callbackExecutor);
  }

  @After
  public void tearDown() {
    ExecutorUtils.close(callbackExecutor, 1000, TimeUnit.MILLISECONDS);
  }

  @Test(timeout = 2000)
  public void testIngestSingleValid() throws InterruptedException {
    when(requestProcessor.process(any())).thenReturn(
        CompletableFuture.completedFuture(Rpc.Status.newBuilder()
            .setStatus(200)
            .build()));
    observer.onNext(Rpc.Ingest.newBuilder().build());
    observer.onCompleted();
    result.getDone().await();
    assertEquals(1, result.getMessages().size());
    final Rpc.Status status = result.getMessages().get(0);
    assertEquals(200, status.getStatus());
  }

  @Test(timeout = 2000)
  public void testIngestSingleRequestProcessorFailure() throws InterruptedException {
    final RuntimeException exception = new RuntimeException("Something went wrong.");
    final CompletableFuture<Rpc.Status> future = new CompletableFuture<>();
    future.completeExceptionally(exception);
    when(requestProcessor.process(any())).thenReturn(future);
    observer.onNext(Rpc.Ingest.newBuilder().build());
    observer.onCompleted();
    result.getDone().await();
    assertEquals(Status.Code.INTERNAL, Status.fromThrowable(result.getError()).getCode());
    assertEquals(exception, Status.fromThrowable(result.getError()).getCause());
  }
}

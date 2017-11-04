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

import com.google.protobuf.ByteString;
import cz.o2.proxima.proto.service.IngestServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.server.grpc.utils.TestStreamObserver;
import cz.o2.proxima.util.ExecutorUtils;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

import static org.junit.Assert.assertEquals;

/**
 * Test server API.
 */
public class IngestServiceTest {

  private RequestProcessor requestProcessor;
  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private ExecutorService callbackExecutor;
  private IngestServiceGrpc.IngestServiceStub ingestStub;

  @Before
  public void setup() throws IOException {
    requestProcessor = mock(RequestProcessor.class);
    callbackExecutor = Executors.newSingleThreadExecutor(
        ExecutorUtils.daemonThreadFactory("test-callback-executor"));
    grpcServer = InProcessServerBuilder
        .forName("test")
        .addService(new IngestService(requestProcessor, callbackExecutor))
        .build()
        .start();
    grpcChannel = InProcessChannelBuilder
        .forName("test")
        .build();
    ingestStub = IngestServiceGrpc.newStub(grpcChannel);
  }

  @After
  public void tearDown() {
    ExecutorUtils.close(callbackExecutor, 1000, TimeUnit.MILLISECONDS);
    grpcServer.shutdownNow();
    grpcChannel.shutdownNow();
  }

  @Test(timeout = 2000)
  public void testIngestBulkInvalidScheme() throws InterruptedException {
    final TestStreamObserver.Result<Rpc.StatusBulk> result = new TestStreamObserver.Result<>();
    final StreamObserver<Rpc.IngestBulk> requestObserver =
        ingestStub.ingestBulk(new TestStreamObserver<>(result));
    when(requestProcessor.process(any()))
        .thenReturn(CompletableFuture.completedFuture(Rpc.Status.newBuilder().build()));

    requestObserver.onNext(bulk(Rpc.Ingest.newBuilder()
        .setEntity("gateway")
        .setAttribute("fail")
        .setKey("gateway1")
        .setValue(ByteString.EMPTY)
        .build()));
    requestObserver.onCompleted();
    result.getDone().await();
    assertEquals(1, result.getMessages().size());
  }

  //  @Test(timeout = 2000)
//  public void testIngestBulkInvalidEntity() throws InterruptedException {
//
//    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
//    result.onNext(bulk(Rpc.Ingest.newBuilder()
//        .setEntity("gateway-invalid")
//        .setAttribute("fail")
//        .setKey("gateway1")
//        .setValue(ByteString.EMPTY)
//        .build()));
//
//    result.onCompleted();
//    latch.await();
//
//    assertEquals(1, responses.size());
//    Rpc.Status status = responses.poll();
//    assertEquals(404, status.getStatus());
//  }
//
//  @Test(timeout = 2000)
//  public void testIngestBulkInvalidEntityAttribute() throws InterruptedException {
//
//    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
//    result.onNext(bulk(Rpc.Ingest.newBuilder()
//        .setEntity("gateway")
//        .setAttribute("fail-invalid")
//        .setKey("gateway1")
//        .setValue(ByteString.EMPTY)
//        .build()));
//
//    result.onCompleted();
//    latch.await();
//
//    assertEquals(1, responses.size());
//    Rpc.Status status = responses.poll();
//    assertEquals(404, status.getStatus());
//  }
//
//  @Test(timeout = 2000)
//  public void testIngestBulkMissingKey() throws InterruptedException {
//
//    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
//    result.onNext(bulk(Rpc.Ingest.newBuilder()
//        .setEntity("dummy")
//        .setAttribute("data")
//        .setUuid(UUID.randomUUID().toString())
//        .setValue(ByteString.EMPTY)
//        .build()));
//
//    result.onCompleted();
//    latch.await();
//
//    assertEquals(1, responses.size());
//    Rpc.Status status = responses.poll();
//    assertEquals(400, status.getStatus());
//
//  }
//
//
//  @Test(timeout = 2000)
//  public void testIngestBulkValid() throws InterruptedException {
//
//    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
//    result.onNext(bulk(Rpc.Ingest.newBuilder()
//        .setEntity("dummy")
//        .setAttribute("data")
//        .setUuid(UUID.randomUUID().toString())
//        .setKey("my-dummy-entity")
//        .setValue(ByteString.EMPTY)
//        .build()));
//
//    result.onCompleted();
//    latch.await();
//
//    assertEquals(1, responses.size());
//    Rpc.Status status = responses.poll();
//    assertEquals(200, status.getStatus());
//
//    InMemStorage storage = (InMemStorage) server.repo.getStorageDescriptor("inmem");
//    Map<String, byte[]> data = storage.getData();
//    assertEquals(1, data.size());
//    assertArrayEquals(
//        new byte[0], data.get("/proxima/dummy/my-dummy-entity#data"));
//  }
//
//  @Test(timeout = 5000)
//  public void testIngestBulkWildcardEntityAttribute() throws Exception {
//
//    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
//    result.onNext(bulk(
//        Rpc.Ingest.newBuilder()
//          .setEntity("dummy")
//          .setAttribute("wildcard.1234")
//          .setKey("dummy1")
//          .setValue(Scheme.Device.newBuilder()
//              .setType("motion")
//              .setPayload(ByteString.copyFromUtf8("muhehe"))
//              .build()
//              .toByteString())
//          .build(),
//        Rpc.Ingest.newBuilder()
//        .setEntity("dummy")
//        .setAttribute("wildcard.12345")
//        .setKey("dummy2")
//        .setValue(Scheme.Device.newBuilder()
//            .setType("motion")
//            .setPayload(ByteString.copyFromUtf8("muhehe"))
//            .build()
//            .toByteString())
//        .build()));
//
//
//    result.onCompleted();
//    latch.await();
//
//    assertEquals(2, responses.size());
//    Rpc.Status status = responses.poll();
//    assertEquals(200, status.getStatus());
//
//    InMemStorage storage = (InMemStorage) server.repo.getStorageDescriptor("inmem");
//    Map<String, byte[]> data = storage.getData();
//    assertEquals(2, data.size());
//    assertEquals("muhehe", Scheme.Device.parseFrom(
//        data.get("/proxima/dummy/dummy1#wildcard.1234"))
//        .getPayload().toStringUtf8());
//    assertEquals("muhehe", Scheme.Device.parseFrom(
//        data.get("/proxima/dummy/dummy2#wildcard.12345"))
//        .getPayload().toStringUtf8());
//  }
//
//  @Test(timeout = 2000)
//  public void testIngestWildcardEntityInvalidScheme() throws InterruptedException {
//
//    StreamObserver<Rpc.IngestBulk> result = ingest.ingestBulk(responseObserver);
//    result.onNext(bulk(Rpc.Ingest.newBuilder()
//        .setEntity("dummy")
//        .setAttribute("wildcard.1234")
//        .setKey("dummy1")
//        .setValue(ByteString.copyFromUtf8("muhehe"))
//        .build()));
//
//    result.onCompleted();
//    latch.await();
//
//    assertEquals(1, responses.size());
//    Rpc.Status status = responses.poll();
//    assertEquals(412, status.getStatus());
//  }
//
  private Rpc.IngestBulk bulk(Rpc.Ingest... ingests) {
    final Rpc.IngestBulk.Builder builder = Rpc.IngestBulk.newBuilder();
    for (Rpc.Ingest ingest : ingests) {
      builder.addIngest(ingest);
    }
    return builder.build();
  }

}

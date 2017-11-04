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

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.grpc.utils.TestStreamObserver;
import cz.o2.proxima.server.test.Test.ExtendedMessage;
import cz.o2.proxima.storage.StreamElement;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test server API.
 */
public class RetrieveServiceTest {

  private final Repository repository = Repository.of(ConfigFactory.load());

  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private RetrieveServiceGrpc.RetrieveServiceStub retrieveStub;

  @Before
  public void setup() throws IOException {
    grpcServer = InProcessServerBuilder
        .forName("test")
        .addService(new RetrieveService(repository))
        .build()
        .start();
    grpcChannel = InProcessChannelBuilder
        .forName("test")
        .build();
    retrieveStub = RetrieveServiceGrpc.newStub(grpcChannel);
  }

  @After
  public void tearDown() {
    grpcServer.shutdownNow();
    grpcChannel.shutdownNow();
  }

  @Test(timeout = 2000)
  public void testGetWithMissingFields() throws InterruptedException {
    final Rpc.GetRequest request = Rpc.GetRequest.newBuilder().build();
    final TestStreamObserver.Result<Rpc.GetResponse> result = new TestStreamObserver.Result<>();
    retrieveStub.get(request, new TestStreamObserver<>(result));
    result.getDone().await();
    assertEquals(1, result.getMessages().size());
    assertEquals(400, result.getMessages().get(0).getStatus());
  }

  @Test(timeout = 2000)
  public void testListWithMissingFields() throws InterruptedException {
    final Rpc.ListRequest request = Rpc.ListRequest.newBuilder().build();
    final TestStreamObserver.Result<Rpc.ListResponse> result = new TestStreamObserver.Result<>();
    retrieveStub.listAttributes(request, new TestStreamObserver<>(result));
    result.getDone().await();
    assertEquals(1, result.getMessages().size());
    final Rpc.ListResponse response = result.getMessages().get(0);
    assertEquals(400, response.getStatus());
  }

  @Test(timeout = 2000)
  public void testGetValid() throws InterruptedException {
    final EntityDescriptor entity = repository
        .findEntity("dummy")
        .orElseThrow(IllegalStateException::new);
    final AttributeDescriptor attribute = entity
        .findAttribute("data")
        .orElseThrow(IllegalStateException::new);
    final String key = "my-fancy-entity-key";
    final StreamElement update = StreamElement.update(entity, attribute, UUID.randomUUID().toString(),
        key, attribute.getName(),
        System.currentTimeMillis(),
        new byte[]{1, 2, 3});
    attribute.getWriter().write(update, (s, err) -> {
      // ~ no-op
    });
    final Rpc.GetRequest request = Rpc.GetRequest.newBuilder()
        .setEntity(entity.getName())
        .setAttribute(attribute.getName())
        .setKey(key)
        .build();
    final TestStreamObserver.Result<Rpc.GetResponse> result = new TestStreamObserver.Result<>();
    retrieveStub.get(request, new TestStreamObserver<>(result));
    result.getDone().await();
    assertEquals(1, result.getMessages().size());
    final Rpc.GetResponse response = result.getMessages().get(0);
    assertEquals("Error: " + response.getStatus() + ": " + response.getStatusMessage(),
        200, response.getStatus());
    assertArrayEquals(new byte[] { 1, 2, 3 }, response.getValue().toByteArray());
  }

  @Test(timeout = 2000)
  public void testGetNotFound() throws InterruptedException {
    final Rpc.GetRequest request = Rpc.GetRequest.newBuilder()
        .setEntity("dummy")
        .setAttribute("dummy")
        .setKey("some-not-existing-key")
        .build();
    final TestStreamObserver.Result<Rpc.GetResponse> result = new TestStreamObserver.Result<>();
    retrieveStub.get(request, new TestStreamObserver<>(result));
    result.getDone().await();
    assertEquals(1, result.getMessages().size());
    final Rpc.GetResponse response = result.getMessages().get(0);
    assertEquals(404, response.getStatus());
  }

  @Test
  public void testListValid() throws Exception {
    final EntityDescriptor entity = repository.findEntity("dummy")
        .orElseThrow(IllegalStateException::new);
    final AttributeDescriptor attribute = entity.findAttribute("wildcard.*")
        .orElseThrow(IllegalStateException::new);

    final String key = "my-fancy-entity-key";

    attribute.getWriter().write(
        StreamElement.update(
            entity, attribute, UUID.randomUUID().toString(),
            key, "wildcard.1",
            System.currentTimeMillis(),
            new byte[] { 1, 2, 3 }),
        (s, err) -> { });

    attribute.getWriter().write(
        StreamElement.update(
            entity, attribute, UUID.randomUUID().toString(),
            key, "wildcard.2",
            System.currentTimeMillis(),
            new byte[] { 1, 2, 3, 4 }),
        (s, err) -> { });

    final Rpc.ListRequest request = Rpc.ListRequest.newBuilder()
        .setEntity(entity.getName())
        .setWildcardPrefix("wildcard")
        .setKey(key)
        .build();

    final TestStreamObserver.Result<Rpc.ListResponse> result = new TestStreamObserver.Result<>();
    retrieveStub.listAttributes(request, new TestStreamObserver<>(result));
    result.getDone().await();

    assertEquals(1, result.getMessages().size());
    final Rpc.ListResponse response = result.getMessages().get(0);
    assertEquals(200, response.getStatus());
    assertEquals(2, response.getValueCount());
    assertEquals("wildcard.1", response.getValue(0).getAttribute());
    assertArrayEquals(new byte[] { 1, 2, 3 }, response.getValue(0)
        .getValue().toByteArray());
    assertEquals("wildcard.2", response.getValue(1).getAttribute());
    assertArrayEquals(new byte[] { 1, 2, 3, 4 }, response.getValue(1)
        .getValue().toByteArray());
  }

  @Test
  public void testListValidWithOffset() throws Exception {
    final EntityDescriptor entity = repository.findEntity("dummy")
        .orElseThrow(IllegalStateException::new);
    final AttributeDescriptor attribute = entity.findAttribute("wildcard.*")
        .orElseThrow(IllegalStateException::new);
    final String key = "my-fancy-entity-key";

    attribute.getWriter().write(
        StreamElement.update(
            entity, attribute, UUID.randomUUID().toString(),
            key, "wildcard.1",
            System.currentTimeMillis(),
            new byte[] { 1, 2, 3 }),
        (s, err) -> { });

    attribute.getWriter().write(
        StreamElement.update(
            entity, attribute, UUID.randomUUID().toString(),
            key, "wildcard.2",
            System.currentTimeMillis(),
            new byte[] { 1, 2, 3, 4 }),
        (s, err) -> { });

    final Rpc.ListRequest request = Rpc.ListRequest.newBuilder()
        .setEntity(entity.getName())
        .setWildcardPrefix("wildcard")
        .setKey(key)
        .setOffset("wildcard.1")
        .build();

    final TestStreamObserver.Result<Rpc.ListResponse> result = new TestStreamObserver.Result<>();
    retrieveStub.listAttributes(request, new TestStreamObserver<>(result));
    result.getDone().await();

    assertEquals(1, result.getMessages().size());
    final Rpc.ListResponse response = result.getMessages().get(0);
    assertEquals(200, response.getStatus());
    assertEquals(1, response.getValueCount());
    assertEquals("wildcard.2", response.getValue(0).getAttribute());
    assertArrayEquals(new byte[] { 1, 2, 3, 4 }, response.getValue(0)
        .getValue().toByteArray());
  }

  @Test
  public void testListValidWithLimit() throws Exception {
    final EntityDescriptor entity = repository.findEntity("dummy")
        .orElseThrow(IllegalStateException::new);
    final AttributeDescriptor attribute = entity.findAttribute("wildcard.*")
        .orElseThrow(IllegalStateException::new);
    final String key = "my-fancy-entity-key";

    attribute.getWriter().write(
        StreamElement.update(
            entity, attribute, UUID.randomUUID().toString(),
            key, "wildcard.1",
            System.currentTimeMillis(),
            new byte[] { 1, 2, 3 }),
        (s, err) -> { });

    attribute.getWriter().write(
        StreamElement.update(
            entity, attribute, UUID.randomUUID().toString(),
            key, "wildcard.2",
            System.currentTimeMillis(),
            new byte[] { 1, 2, 3, 4 }),
        (s, err) -> { });

    final Rpc.ListRequest request = Rpc.ListRequest.newBuilder()
        .setEntity(entity.getName())
        .setWildcardPrefix("wildcard")
        .setKey(key)
        .setLimit(1)
        .build();

    final TestStreamObserver.Result<Rpc.ListResponse> result = new TestStreamObserver.Result<>();
    retrieveStub.listAttributes(request, new TestStreamObserver<>(result));
    result.getDone().await();

    assertEquals(1, result.getMessages().size());
    final Rpc.ListResponse response = result.getMessages().get(0);
    assertEquals(200, response.getStatus());
    assertEquals(1, response.getValueCount());
    assertEquals("wildcard.1", response.getValue(0).getAttribute());
    assertArrayEquals(new byte[] { 1, 2, 3 }, response.getValue(0)
        .getValue().toByteArray());
  }

  @Test
  public void testGetValidExtendedScheme()
      throws InterruptedException, InvalidProtocolBufferException {

    final EntityDescriptor entity = repository.findEntity("test")
        .orElseThrow(IllegalStateException::new);
    final AttributeDescriptor attribute = entity.findAttribute("data")
        .orElseThrow(IllegalStateException::new);
    String key = "my-fancy-entity-key";
    final cz.o2.proxima.server.test.Test.ExtendedMessage payload = ExtendedMessage.newBuilder()
        .setFirst(1).setSecond(2).build();

    attribute.getWriter().write(
        StreamElement.update(entity, attribute, UUID.randomUUID().toString(),
            key, attribute.getName(),
            System.currentTimeMillis(),
            payload.toByteArray()),
        (s, err) -> { });

    final Rpc.GetRequest request = Rpc.GetRequest.newBuilder()
        .setEntity(entity.getName())
        .setAttribute(attribute.getName())
        .setKey(key)
        .build();

    final TestStreamObserver.Result<Rpc.GetResponse> result = new TestStreamObserver.Result<>();
    retrieveStub.get(request, new TestStreamObserver<>(result));
    result.getDone().await();

    assertEquals(1, result.getMessages().size());
    final Rpc.GetResponse response = result.getMessages().get(0);
    assertEquals(
        "Error: " + response.getStatus() + ": " + response.getStatusMessage(),
        200, response.getStatus());
    assertEquals(payload, ExtendedMessage.parseFrom(
        response.getValue().toByteArray()));
  }

  @Test
  public void testListNotFound() throws Exception {
    // FIXME
  }

  @Test
  public void testListEmpty() throws Exception {
    // FIXME
  }
}

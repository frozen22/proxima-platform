/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.pubsub;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import static cz.o2.proxima.direct.pubsub.Util.delete;
import static cz.o2.proxima.direct.pubsub.Util.deleteWildcard;
import static cz.o2.proxima.direct.pubsub.Util.update;
import cz.o2.proxima.internal.shaded.com.google.common.collect.Sets;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorImpl;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite for {@link PubSubReader}.
 */
public class PubSubReaderTest {

  private final Repository repo = ConfigRepository.of(ConfigFactory.load().resolve());
  private final DirectDataOperator direct = repo.asDataOperator(
      DirectDataOperator.class, op -> op.withExecutorFactory(
          () -> Executors.newCachedThreadPool()));
  private final Context context = direct.getContext();
  private final AttributeDescriptorImpl<?> attr;
  private final AttributeDescriptorImpl<?> wildcard;
  private final EntityDescriptor entity;
  private final PubSubAccessor accessor;
  private TestPubSubReader reader;

  private class TestPubSubReader extends PubSubReader {

    private final Context context;
    private final Set<Integer> acked = new HashSet<>();
    private final Set<Integer> nacked = new HashSet<>();

    private Supplier<PubsubMessage> supplier;

    public TestPubSubReader(Context context) {
      super(accessor, context);
      this.context = context;
    }

    void setSupplier(Supplier<PubsubMessage> supplier) {
      this.supplier = supplier;
    }

    @Override
    Subscriber newSubscriber(
        ProjectSubscriptionName subscription,
        MessageReceiver receiver) {

      return MockSubscriber.create(
          subscription, receiver, supplier, acked, nacked,
          context.getExecutorService());
    }

  }

  public PubSubReaderTest() throws URISyntaxException {
    this.attr = AttributeDescriptor.newBuilder(repo)
        .setEntity("entity")
        .setName("attr")
        .setSchemeUri(new URI("bytes:///"))
        .build();
    this.wildcard = AttributeDescriptor.newBuilder(repo)
        .setEntity("entity")
        .setName("wildcard.*")
        .setSchemeUri(new URI("bytes:///"))
        .build();
    this.entity = EntityDescriptor.newBuilder()
        .setName("entity")
        .addAttribute(attr)
        .addAttribute(wildcard)
        .build();
    assertTrue(entity.findAttribute("attr").isPresent());
    this.accessor = new PubSubAccessor(
        entity, new URI("gps://my-project/topic"), Collections.emptyMap());
  }

  @Before
  public void setUp() {
    reader = new TestPubSubReader(context);
  }

  @Test(timeout = 10000)
  public void testObserve() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs = new LinkedList<>(
        Arrays.asList(
            update("key1", "attr", new byte[] { 1, 2 }, now),
            delete("key2", "attr", now + 1000),
            deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(() -> {
      if (inputs.isEmpty()) {
        LockSupport.park();
      }
      return inputs.pop();
    });
    List<StreamElement> elems = new ArrayList<>();
    AtomicBoolean cancelled = new AtomicBoolean();
    CountDownLatch latch = new CountDownLatch(3);
    ObserveHandle handle = reader.observe("dummy", new LogObserver() {
      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        elems.add(ingest);
        context.confirm();
        latch.countDown();
        return true;
      }

      @Override
      public void onCancelled() {
        cancelled.set(true);
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }
    });
    latch.await();
    handle.cancel();
    assertEquals(3, elems.size());
    StreamElement elem = elems.get(0);
    assertEquals("key1", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertFalse(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertArrayEquals(new byte[] { 1, 2 }, elem.getValue());
    assertEquals(now, elem.getStamp());
    elem = elems.get(1);
    assertEquals("key2", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertEquals(now + 1000L, elem.getStamp());
    elem = elems.get(2);
    assertEquals("key3", elem.getKey());
    assertEquals(wildcard.toAttributePrefix() + "*", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertTrue(elem.isDeleteWildcard());
    assertEquals(now, elem.getStamp());

    assertTrue(cancelled.get());
    assertEquals(Sets.newHashSet(0, 1, 2), reader.acked);
  }

  @Test(timeout = 10000)
  public void testObserveError() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs = new LinkedList<>(
        Arrays.asList(
            update("key1", "attr", new byte[] { 1, 2 }, now),
            delete("key2", "attr", now + 1000),
            deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(() -> {
      if (inputs.isEmpty()) {
        LockSupport.park();
      }
      return inputs.pop();
    });
    final AtomicBoolean cancelled = new AtomicBoolean();
    final CountDownLatch latch = new CountDownLatch(3);
    final ObserveHandle handle = reader.observe("dummy", new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        throw new RuntimeException("Fail");
      }

      @Override
      public void onCancelled() {
        cancelled.set(true);
      }

      @Override
      public boolean onError(Throwable error) {
        assertEquals("Fail", error.getCause().getMessage());
        latch.countDown();
        return true;
      }
    });
    latch.await();
    assertTrue(reader.acked.isEmpty());
    assertFalse(reader.nacked.isEmpty());
    handle.cancel();
  }

  @Test(timeout = 10000)
  public void testObserveBulk() throws InterruptedException {
    long now = System.currentTimeMillis();
    Deque<PubsubMessage> inputs = new LinkedList<>(
        Arrays.asList(
            update("key1", "attr", new byte[] { 1, 2 }, now),
            delete("key2", "attr", now + 1000),
            deleteWildcard("key3", wildcard, now)));
    reader.setSupplier(() -> {
      if (inputs.isEmpty()) {
        LockSupport.park();
      }
      return inputs.pop();
    });
    List<StreamElement> elems = new ArrayList<>();
    AtomicBoolean cancelled = new AtomicBoolean();
    CountDownLatch latch = new CountDownLatch(3);
    AtomicReference<OffsetCommitter> commit = new AtomicReference<>();
    ObserveHandle handle = reader.observeBulk("dummy", new LogObserver() {
      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        elems.add(ingest);
        commit.set(context);
        latch.countDown();
        return true;
      }

      @Override
      public void onCancelled() {
        cancelled.set(true);
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }
    });
    latch.await();
    commit.get().confirm();
    handle.cancel();
    assertEquals(3, elems.size());
    StreamElement elem = elems.get(0);
    assertEquals("key1", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertFalse(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertArrayEquals(new byte[] { 1, 2 }, elem.getValue());
    assertEquals(now, elem.getStamp());
    elem = elems.get(1);
    assertEquals("key2", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertEquals(now + 1000L, elem.getStamp());
    elem = elems.get(2);
    assertEquals("key3", elem.getKey());
    assertEquals(wildcard.toAttributePrefix() + "*", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertTrue(elem.isDeleteWildcard());
    assertEquals(now, elem.getStamp());

    assertTrue(cancelled.get());
    assertEquals(Sets.newHashSet(0, 1, 2), reader.acked);
  }

}

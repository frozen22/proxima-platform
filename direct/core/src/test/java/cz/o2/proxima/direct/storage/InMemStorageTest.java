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
package cz.o2.proxima.direct.storage;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test suite for {@link InMemStorage}.
 */
public class InMemStorageTest implements Serializable {

  final Repository repo = ConfigRepository.of(
      ConfigFactory.load("test-reference.conf").resolve());
  final DirectDataOperator direct = repo.asDataOperator(DirectDataOperator.class);
  final EntityDescriptor entity = repo
      .findEntity("dummy")
      .orElseThrow(() -> new IllegalStateException("Missing entity dummy"));

  @Test(timeout = 10000)
  public void testObservePartitions()
      throws URISyntaxException, InterruptedException {

    InMemStorage storage = new InMemStorage();
    DataAccessor accessor = storage.create(
        entity, new URI("inmem:///inmemstoragetest"),
        Collections.emptyMap());
    CommitLogReader reader = accessor.getCommitLogReader(direct.getContext())
        .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    AttributeWriterBase writer = accessor.getWriter(direct.getContext())
        .orElseThrow(() -> new IllegalStateException("Missing writer"));
    AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    ObserveHandle handle = reader.observePartitions(
        reader.getPartitions(), new LogObserver() {

          @Override
          public void onRepartition(OnRepartitionContext context) {
            assertEquals(1, context.partitions().size());
            latch.set(new CountDownLatch(1));
          }

          @Override
          public boolean onNext(
              StreamElement ingest, OnNextContext context) {

            assertEquals(0, context.getPartition().getId());
            assertEquals("key", ingest.getKey());
            context.confirm();
            latch.get().countDown();
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }

        });

    writer.online().write(
        StreamElement.update(
            entity, entity.findAttribute("data")
                .orElseThrow(() -> new IllegalStateException(
                    "Missing attribute data")),
            UUID.randomUUID().toString(), "key", "data",
            System.currentTimeMillis(), new byte[] { 1, 2, 3}),
        (succ, exc) -> { });
    latch.get().await();
  }

}

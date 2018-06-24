/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Test suite for {@link StreamElement}.
 */
public class StreamElementTest {

  private final Repository repo = ConfigRepository.Builder
      .ofTest(ConfigFactory.empty())
      .build();

  private final AttributeDescriptorBase<byte[]> attr = AttributeDescriptor.newBuilder(repo)
      .setEntity("entity")
      .setName("attr")
      .setSchemeURI(URI.create("bytes:///"))
      .build();

  private final AttributeDescriptorBase<byte[]> attrWildcard = AttributeDescriptor.newBuilder(repo)
      .setEntity("entity")
      .setName("wildcard.*")
      .setSchemeURI(URI.create("bytes:///"))
      .build();

  private final EntityDescriptor entity = EntityDescriptor.newBuilder()
      .setName("entity")
      .addAttribute(attr)
      .addAttribute(attrWildcard)
      .build();

  @Test
  public void testUpdate() {
    final long now = System.currentTimeMillis();
    final StreamElement<byte[]> update = StreamElement.of(entity, attr)
        .uuid(UUID.randomUUID().toString())
        .key("key")
        .timestamp(now)
        .attribute(attr.getName())
        .updateRaw(new byte[]{1, 2});
    assertFalse(update.isDelete());
    assertFalse(update.isDeleteWildcard());
    assertEquals("key", update.getKey());
    assertEquals(attr.getName(), update.getAttribute());
    assertEquals(attr, update.getAttributeDescriptor());
    assertEquals(entity, update.getEntityDescriptor());
    assertArrayEquals(new byte[]{1, 2}, update.getValue());
    assertEquals(now, update.getStamp());
  }

  @Test
  public void testDelete() {
    final long now = System.currentTimeMillis();
    final StreamElement<byte[]> delete = StreamElement.of(entity, attr)
        .uuid(UUID.randomUUID().toString())
        .key("key")
        .timestamp(now)
        .attribute(attr.getName())
        .delete();
    assertTrue(delete.isDelete());
    assertFalse(delete.isDeleteWildcard());
    assertEquals("key", delete.getKey());
    assertEquals(attr.getName(), delete.getAttribute());
    assertEquals(attr, delete.getAttributeDescriptor());
    assertEquals(entity, delete.getEntityDescriptor());
    assertNull(delete.getValue());
    assertEquals(now, delete.getStamp());
  }

  @Test
  public void testDeleteWildcard() {
    final long now = System.currentTimeMillis();
    final StreamElement<?> delete = StreamElement.of(entity, attrWildcard)
        .uuid(UUID.randomUUID().toString())
        .key("key")
        .timestamp(now)
        .deleteWildcard();
    assertTrue(delete.isDelete());
    assertTrue(delete.isDeleteWildcard());
    assertEquals("key", delete.getKey());
    assertNull(attrWildcard.getName(), delete.getAttribute());
    assertEquals(attrWildcard, delete.getAttributeDescriptor());
    assertEquals(entity, delete.getEntityDescriptor());
    assertNull(delete.getValue());
    assertEquals(now, delete.getStamp());
  }

}

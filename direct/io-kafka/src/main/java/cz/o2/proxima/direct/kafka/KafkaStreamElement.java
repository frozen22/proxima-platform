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
package cz.o2.proxima.direct.kafka;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/** Data read from a kafka partition. */
@Slf4j
public class KafkaStreamElement extends StreamElement {

  public static class KafkaStreamElementSerializer implements ElementSerializer<String, byte[]> {

    @Nullable
    @Override
    public StreamElement read(ConsumerRecord<String, byte[]> record, EntityDescriptor entityDesc) {
      String key = record.key();
      byte[] value = record.value();
      // in kafka, each entity attribute is separated by `#' from entity key
      int hashPos = key.lastIndexOf('#');
      if (hashPos < 0 || hashPos >= key.length()) {
        log.error("Invalid key in kafka topic: {}", key);
      } else {
        String entityKey = key.substring(0, hashPos);
        String attribute = key.substring(hashPos + 1);
        Optional<AttributeDescriptor<Object>> attr =
            entityDesc.findAttribute(attribute, true /* allow reading protected */);
        if (!attr.isPresent()) {
          log.error("Invalid attribute {} in kafka key {}", attribute, key);
        } else {
          return new KafkaStreamElement(
              entityDesc,
              attr.get(),
              String.valueOf(record.topic() + "#" + record.partition() + "#" + record.offset()),
              entityKey,
              attribute,
              record.timestamp(),
              value,
              record.partition(),
              record.offset());
        }
      }
      return null;
    }

    @Override
    public Pair<String, byte[]> write(StreamElement data) {
      return Pair.of(data.getKey() + "#" + data.getAttribute(), data.getValue());
    }

    @Override
    public Serde<String> keySerde() {
      return Serdes.String();
    }

    @Override
    public Serde<byte[]> valueSerde() {
      return Serdes.ByteArray();
    }
  }

  @Getter private final int partition;
  @Getter private final long offset;

  KafkaStreamElement(
      EntityDescriptor entityDesc,
      AttributeDescriptor attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp,
      byte[] value,
      int partition,
      long offset) {

    super(
        entityDesc,
        attributeDesc,
        uuid,
        key,
        attribute,
        stamp,
        false /* not forced, is inferred from attribute descriptor name */,
        value);

    this.partition = partition;
    this.offset = offset;
  }

  @Override
  public String toString() {
    return "KafkaStreamElement(entityDesc="
        + getEntityDescriptor()
        + ", attributeDesc="
        + getAttributeDescriptor()
        + ", uuid="
        + getUuid()
        + ", key="
        + getKey()
        + ", attribute="
        + getAttribute()
        + ", stamp="
        + getStamp()
        + ", value.length="
        + (getValue() == null ? 0 : getValue().length)
        + ", partition="
        + partition
        + ", offset="
        + offset
        + ")";
  }
}

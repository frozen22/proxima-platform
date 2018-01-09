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
package cz.o2.proxima.example;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import cz.o2.proxima.example.event.Event.BaseEvent;
import cz.o2.proxima.storage.kafka.Partitioner;
import lombok.extern.slf4j.Slf4j;

/**
 * Partitioner for the events.
 */
@Slf4j
public class EventPartitioner implements Partitioner {

  @Override
  public int getPartitionId(String key, String attribute, byte[] value) {
    if (value == null) {
      log.warn("Received event with null value!");
    } else {
      try {
        BaseEvent event = BaseEvent.parseFrom(value);
        int partition = event.getUserName().hashCode();
        if (log.isDebugEnabled()) {
          log.debug(
              "Partitioned event {} to partition ID {}",
              TextFormat.shortDebugString(event), partition);
        }
        return partition;
      } catch (InvalidProtocolBufferException ex) {
        log.warn("Failed to parse event", ex);
      }
    }
    return 0;
  }

}

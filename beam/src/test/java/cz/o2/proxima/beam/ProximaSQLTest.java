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
package cz.o2.proxima.beam;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;

public class ProximaSQLTest {

  @Test
  public void test() {

    final Pipeline pipeline = Pipeline.create();

    final Repository repository = Repository.of(
        ConfigFactory.load(ConfigFactory.load("test-sql.conf")));

    final EntityDescriptor entityDescriptor =
        Optionals.get(repository.findEntity("event"));

    final AttributeDescriptor<Event> attributeDescriptor =
        Optionals.get(entityDescriptor.findAttribute("data"));

    pipeline.getCoderRegistry().registerCoderProvider(ProximaCoderProvider.of(repository));

    final PCollection<StreamElement<Event>> input = pipeline.apply(Create.of(
        StreamElement.of(entityDescriptor, attributeDescriptor)
            .uuid(UUID.randomUUID().toString())
            .key("key")
            .timestamp(123L)
            .attribute("data")
            .update(new Event("first", true)),
        StreamElement.of(entityDescriptor, attributeDescriptor)
            .uuid(UUID.randomUUID().toString())
            .key("key2")
            .timestamp(123L)
            .attribute("data")
            .update(new Event("second", false)),
        StreamElement.of(entityDescriptor, attributeDescriptor)
            .uuid(UUID.randomUUID().toString())
            .key("key3")
            .timestamp(123L)
            .attribute("data")
            .update(new Event("third", true))));

    final PCollection<Event> events = input.apply(new ProximaSQL.ExtractElement<>())
        .setTypeDescriptor(TypeDescriptor.of(Event.class));

    final PCollection<Row> table =
        events.apply(ProximaSQL.ExtractorRow.of(new AttachedRowExtractor<>()))
            .setCoder(Event.ROW_TYPE.getRowCoder());

    final PCollection<Row> result = table
        .apply(BeamSql.query("SELECT name FROM PCOLLECTION WHERE important = true"));

    PAssert.that(result).satisfies(rows -> {
      rows.forEach(System.out::println);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }
}

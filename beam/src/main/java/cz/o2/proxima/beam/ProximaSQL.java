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
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

public class ProximaSQL {

  public static void main(String[] args) {

    final Repository repository = Repository.of(ConfigFactory.load());
    final Pipeline pipeline = Pipeline.create();

    pipeline.apply(BeamSql.query("my query ..."));

    pipeline.getCoderRegistry().registerCoderProvider(ProximaCoderProvider.of(repository));
  }

  static class ExtractElement<T> extends PTransform<PCollection<StreamElement<T>>, PCollection<T>> {

    @Override
    public PCollection<T> expand(PCollection<StreamElement<T>> input) {
      return input.apply(ParDo.of(new DoFn<StreamElement<T>, T>() {

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(ProcessContext context) {
          final StreamElement<T> streamElement = context.element();
          final T parsed = Optionals.get(streamElement.getParsed());
          context.outputWithTimestamp(parsed, new Instant(streamElement.getStamp()));
        }
      }));
    }
  }

  static class ExtractorRow<T> extends PTransform<PCollection<T>, PCollection<Row>> {

    public static <T> ExtractorRow<T> of(RowExtractor<T> extractor) {
      return new ExtractorRow<>(extractor);
    }

    private final RowExtractor<T> extractor;

    private ExtractorRow(RowExtractor<T> extractor) {
      this.extractor = extractor;
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {

      return input.apply(ParDo.of(new DoFn<T, Row>() {

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(ProcessContext context) {
          context.outputWithTimestamp(extractor.extract(context.element()), context.timestamp());
        }
      }));
    }
  }
}

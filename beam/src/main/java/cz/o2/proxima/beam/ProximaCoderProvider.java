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

import com.google.common.io.ByteStreams;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.util.Optionals;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ProximaCoderProvider extends CoderProvider {

  public static ProximaCoderProvider of(Repository repository) {
   return new ProximaCoderProvider(repository.getAllEntities()
        .flatMap(entity -> entity.getAllAttributes().stream())
        .collect(Collectors.toMap(attribute -> {
          try {
            final URI uri = attribute.getSchemeURI();
            return Class.forName(uri.getSchemeSpecificPart());
          } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Unable to resolve class for '"
                    + attribute.getEntity()
                    + "."
                    + attribute.getName()
                    + "'.");
          }
        }, Function.identity())));
  }

  public static class ProximaCoder<T> extends CustomCoder<T> {

    private final ValueSerializer<T> serializer;

    private ProximaCoder(ValueSerializer<T> serializer) {
      this.serializer = serializer;
    }

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
      outStream.write(serializer.serialize(value));
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
      // TODO serializer should accept streams
      final byte[] bytes = ByteStreams.toByteArray(inStream);
      return Optionals.get(serializer.deserialize(bytes));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      // deterministic
    }
  }


  private final Map<Class<?>, AttributeDescriptor<?>> descriptors;

  private ProximaCoderProvider(Map<Class<?>, AttributeDescriptor<?>> descriptors) {
    this.descriptors = descriptors;
  }

  @Override
  public <T> Coder<T> coderFor(
      TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders) throws
      CannotProvideCoderException {

    @SuppressWarnings("unchecked")
    final AttributeDescriptor<T> attributeDescriptor =
        (AttributeDescriptor<T>) descriptors.get(typeDescriptor.getRawType());

    if (attributeDescriptor == null) {
      throw new CannotProvideCoderException("Type is not registered in repository.");
    }

    return new ProximaCoder<>(attributeDescriptor.getValueSerializer());
  }

}

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
package cz.o2.proxima.scheme;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Plain java object serializer. Please not it is not intended for production use.
 */
@Slf4j
public class JavaValueSerializerFactory implements ValueSerializerFactory {

  @Override
  public String getAcceptableScheme() {
    return "java";
  }

  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI uri) {

    final Class<?> clazz;
    try {
      clazz = Class.forName(uri.getSchemeSpecificPart());
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Unable to create serializer.", e);
    }

    if (Serializable.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Class '" + clazz.getName() + "' is not serializable.");
    }

    return new ValueSerializer<T>() {

      @SuppressWarnings("unchecked")
      @Override
      public Optional<T> deserialize(byte[] input) {

        try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(input))) {
          return Optional.of((T) ois.readObject());
        } catch (IOException | ClassNotFoundException e) {
          log.warn("Unable to deserialize value of '" + uri + "'.");
          return Optional.empty();
        }
      }

      @Override
      public byte[] serialize(T value) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
             final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
          oos.writeObject(value);
          return baos.toByteArray();
        } catch (IOException e) {
          throw new IllegalStateException("Unable to serialize value of '" + uri + "'.");
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public T getDefault() {
        try {
          return (T) clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new IllegalStateException("Unable to create default value of '" + uri + "'.");
        }
      }
    };
  }
}

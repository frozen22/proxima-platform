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
package cz.o2.proxima.scheme.proto;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Parser;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.util.Classpath;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializer from protobuffers.
 */
@Slf4j
public class ProtoSerializerFactory implements ValueSerializerFactory {

  private final Map<URI, ValueSerializer<?>> parsers = new ConcurrentHashMap<>();

  @Override
  public String getAcceptableScheme() {
    return "proto";
  }

  @SuppressWarnings("unchecked")
  private static <M extends AbstractMessage> ValueSerializer<M>
      createSerializer(URI uri) {
    return new ValueSerializer<M>() {

      final String protoClass = uri.getSchemeSpecificPart();
      @Nullable
      transient M defVal = null;

      transient Parser<?> parser = null;

      @Override
      public Optional<M> deserialize(byte[] input) {
        if (parser == null) {
          parser = getParserForClass(protoClass);
        }
        try {
          return Optional.of((M) parser.parseFrom(input));
        } catch (Exception ex) {
          log.debug("Failed to parse input bytes", ex);
        }
        return Optional.empty();
      }

      @Override
      public M getDefault() {
        if (defVal == null) {
          defVal = getDefaultInstance(protoClass);
        }
        return defVal;
      }

      @Override
      public byte[] serialize(M value) {
        return value.toByteArray();
      }


      @SuppressWarnings("unchecked")
      private Parser<?> getParserForClass(String protoClassName) {

        try {
          Class<?> proto = Classpath
              .findClass(protoClassName, AbstractMessage.class);
          Method p = proto.getMethod("parser");
          return (Parser) p.invoke(null);
        } catch (IllegalAccessException | IllegalArgumentException
            | NoSuchMethodException | SecurityException | InvocationTargetException ex) {

          throw new IllegalArgumentException(
              "Cannot create parser from class " + protoClassName, ex);
        }
      }

    };
  }
  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI scheme) {
    return (ValueSerializer) parsers.computeIfAbsent(
        scheme, ProtoSerializerFactory::createSerializer);
  }

  @SuppressWarnings("unchecked")
  static <M extends AbstractMessage> M getDefaultInstance(String protoClass) {
    try {
      Class<AbstractMessage> cls = Classpath.findClass(
          protoClass, AbstractMessage.class);
      Method method = cls.getMethod("getDefaultInstance");
      return (M) method.invoke(null);
    } catch (Exception ex) {
      throw new IllegalArgumentException(
          "Cannot retrieve default instance for type " + protoClass);
    }
  }

}

/**
 * Copyright 2017 O2 Czech Republic, a.s.
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
package cz.o2.proxima.server.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class RetrieveService extends RetrieveServiceGrpc.RetrieveServiceImplBase {

  private final Repository repository;

  public RetrieveService(Repository repository) {
    this.repository = repository;
  }

  @Override
  public void get(Rpc.GetRequest request,
                  StreamObserver<Rpc.GetResponse> responseObserver) {
    Metrics.GET_REQUESTS.increment();
    responseObserver.onNext(get(request));
    responseObserver.onCompleted();
  }

  @Override
  public void listAttributes(Rpc.ListRequest request,
                             StreamObserver<Rpc.ListResponse> responseObserver) {
    Metrics.LIST_REQUESTS.increment();
    responseObserver.onNext(listAttributes(request));
    responseObserver.onCompleted();
  }

  private Rpc.GetResponse get(Rpc.GetRequest request) {
    log.debug("Processing get {}", TextFormat.shortDebugString(request));
    final Rpc.GetResponse.Builder builder = Rpc.GetResponse.newBuilder();
    if (!RequestValidator.isValid(request)) {
      return builder.setStatus(400)
          .setStatusMessage("Missing some required fields")
          .build();
    }

    final Optional<EntityDescriptor> entity = repository.findEntity(request.getEntity());
    if (!entity.isPresent()) {
      return builder.setStatus(404)
          .setStatusMessage("Entity " + request.getEntity() + " not found")
          .build();
    }

    final AttributeDescriptor attribute = entity.get()
        .findAttribute(request.getAttribute())
        .orElse(null);

    if (attribute == null) {
      return builder.setStatus(404)
          .setStatusMessage("Entity " + request.getEntity()
              + " does not have attribute "
              + request.getAttribute())
          .build();
    }

    final RandomAccessReader reader = repository.getFamiliesForAttribute(attribute)
        .stream()
        .map(AttributeFamilyDescriptor::getRandomAccessReader)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst()
        .orElse(null);

    if (reader == null) {
      return builder.setStatus(400)
          .setStatusMessage("Attribute " + attribute
              + " has no random access reader")
          .build();
    }

    final KeyValue<?> kv = reader
        .get(request.getKey(), request.getAttribute(), attribute)
        .orElse(null);

    if (kv == null) {
      return builder.setStatus(404)
          .setStatusMessage("Key " + request.getKey() + " and/or attribute "
              + request.getAttribute() + " not found")
          .build();
    }

    return builder.setStatus(200)
        .setValue(ByteString.copyFrom(kv.getValueBytes()))
        .build();
  }

  private Rpc.ListResponse listAttributes(Rpc.ListRequest request) {
    log.debug("Processing listAttributes {}", TextFormat.shortDebugString(request));
    final Rpc.ListResponse.Builder builder = Rpc.ListResponse.newBuilder();

    if (!RequestValidator.isValid(request)) {
      return builder.setStatus(400)
          .setStatusMessage("Missing some required fields.")
          .build();
    }

    final EntityDescriptor entity = repository
        .findEntity(request.getEntity())
        .orElse(null);

    if (entity == null) {
      return builder.setStatus(404)
          .setStatusMessage("Entity " + request.getEntity() + " not found")
          .build();
    }

    final AttributeDescriptor wildcard = entity
        .findAttribute(request.getWildcardPrefix() + ".*")
        .orElse(null);

    if (wildcard == null) {
      return builder.setStatus(404)
          .setStatusMessage("Entity " + request.getEntity()
              + " does not have wildcard attribute "
              + request.getWildcardPrefix())
          .build();
    }

    final RandomAccessReader reader = repository.getFamiliesForAttribute(wildcard)
        .stream()
        .map(AttributeFamilyDescriptor::getRandomAccessReader)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst()
        .orElse(null);

    if (reader == null) {
      return builder.setStatus(404)
          .setStatusMessage("Attribute " + wildcard
              + " has no random access reader")
          .build();
    }

    builder.setStatus(200);

    final RandomAccessReader.Offset offset = reader
        .fetchOffset(RandomAccessReader.Listing.ATTRIBUTE, request.getOffset());

    final int limit = request.getLimit() > 0 ? request.getLimit() : -1;

    reader.scanWildcard(request.getKey(), wildcard, offset, limit, kv ->
        builder.addValue(Rpc.ListResponse.AttrValue
            .newBuilder()
            .setAttribute(kv.getAttribute())
            .setValue(ByteString.copyFrom(kv.getValueBytes()))));

    return builder.build();
  }

}

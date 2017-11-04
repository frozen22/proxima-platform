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

import com.google.protobuf.TextFormat;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class RequestProcessorImpl implements RequestProcessor {

  private static final Logger log = LoggerFactory.getLogger(RequestProcessorImpl.class);

  private final Repository repository;

  public RequestProcessorImpl(Repository repository) {
    this.repository = repository;
  }

  @Override
  public CompletableFuture<Rpc.Status> process(Rpc.Ingest request) {
    Metrics.INGESTS.increment();
    log.debug("Processing request {}.", TextFormat.shortDebugString(request));

    final Rpc.Status.Builder builder = Rpc.Status.newBuilder()
        .setUuid(request.getUuid());

    // ~ validate

    if (!RequestValidator.isValid(request)) {
      return CompletableFuture.completedFuture(builder
          .setStatus(400)
          .setStatusMessage("Missing required fields in input message")
          .build());
    }
    final Optional<EntityDescriptor> entity = repository.findEntity(request.getEntity());
    if (!entity.isPresent()) {
      return CompletableFuture.completedFuture(builder
          .setStatus(404)
          .setStatusMessage("Entity " + request.getEntity() + " not found")
          .build());
    }
    final Optional<AttributeDescriptor> attr = entity.get()
        .findAttribute(request.getAttribute());
    if (!attr.isPresent()) {
      return CompletableFuture.completedFuture(builder
          .setStatus(404)
          .setStatusMessage("Attribute " + request.getAttribute() + " of entity "
              + entity.get().getName() + " not found")
          .build());
    }

    final StreamElement streamElement = toStreamElement(request, entity.get(), attr.get());

    final EntityDescriptor entityDesc = streamElement.getEntityDescriptor();
    final AttributeDescriptor attributeDesc = streamElement.getAttributeDescriptor();

    final OnlineAttributeWriter writerBase = attributeDesc.getWriter();
    // we need online writer here
    final OnlineAttributeWriter writer = writerBase == null ? null : writerBase.online();

    if (writer == null) {
      log.warn("Missing writer for request {}", streamElement);
      return CompletableFuture.completedFuture(builder
          .setStatus(503)
          .setStatusMessage("No writer for attribute " + attributeDesc.getName())
          .build());
    }

    // delete is always valid
    final boolean valid = streamElement.isDelete()
        || attributeDesc.getValueSerializer().isValid(streamElement.getValue());

    if (!valid) {
      log.info("Request {} is not valid", streamElement);
      return CompletableFuture.completedFuture(builder
          .setStatus(412)
          .setStatusMessage("Invalid scheme for " + entityDesc.getName()
              + "." + attributeDesc.getName())
          .build());
    }

    if (streamElement.isDelete()) {
      if (streamElement.isDeleteWildcard()) {
        Metrics.DELETE_WILDCARD_REQUESTS.increment();
      } else {
        Metrics.DELETE_REQUESTS.increment();
      }
    } else {
      Metrics.UPDATE_REQUESTS.increment();
    }

    Metrics.COMMIT_LOG_APPEND.increment();
    // write the ingest into the commit log and confirm to the client
    log.debug("Writing request {} to commit log {}", streamElement, writerBase.getURI());

    final CompletableFuture<Rpc.Status> future = new CompletableFuture<>();

    // @todo better
    writerBase.write(streamElement, (s, error) -> {
      if (s) {
        future.complete(builder
            .setStatus(200)
            .build());
      } else {
        future.complete(builder
            .setStatus(500)
            .setStatusMessage(error.getMessage())
            .build());
      }
    });

    return future;
  }

  private static StreamElement toStreamElement(
      Rpc.Ingest request, EntityDescriptor entity, AttributeDescriptor attr) {

    final long stamp = request.getStamp() == 0
        ? System.currentTimeMillis()
        : request.getStamp();

    if (request.getDelete()) {
      return attr.isWildcard() && attr.getName().equals(request.getAttribute())
          ? StreamElement.deleteWildcard(
          entity, attr, request.getUuid(),
          request.getKey(), stamp)
          : StreamElement.delete(
          entity, attr, request.getUuid(),
          request.getKey(), request.getAttribute(), stamp);
    }
    return StreamElement.update(
        entity, attr, request.getUuid(),
        request.getKey(), request.getAttribute(),
        stamp,
        request.getValue().toByteArray());
  }
}

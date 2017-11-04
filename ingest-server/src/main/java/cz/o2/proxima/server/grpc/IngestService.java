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

import cz.o2.proxima.proto.service.IngestServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ExecutorService;

/**
 * Ingest gRPC service.
 **/
public class IngestService extends IngestServiceGrpc.IngestServiceImplBase {

  private final RequestProcessor requestProcessor;
  private final ExecutorService callbackExecutor;

  public IngestService(RequestProcessor requestProcessor, ExecutorService callbackExecutor) {
    this.requestProcessor = requestProcessor;
    this.callbackExecutor = callbackExecutor;
  }

  @Override
  public void ingest(Rpc.Ingest request, StreamObserver<Rpc.Status> responseObserver) {
    final IngestSingleStreamObserver requestObserver =
        new IngestSingleStreamObserver(responseObserver, requestProcessor, callbackExecutor);
    requestObserver.onNext(request);
    requestObserver.onCompleted();
  }

  @Override
  public StreamObserver<Rpc.Ingest> ingestSingle(StreamObserver<Rpc.Status> responseObserver) {
    return new IngestSingleStreamObserver(responseObserver, requestProcessor, callbackExecutor);
  }

  @Override
  public StreamObserver<Rpc.IngestBulk> ingestBulk(
      StreamObserver<Rpc.StatusBulk> responseObserver) {
    return new IngestBulkStreamObserver(responseObserver, requestProcessor, callbackExecutor);
  }
}

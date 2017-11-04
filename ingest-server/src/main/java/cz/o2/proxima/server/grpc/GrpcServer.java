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

import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import cz.o2.proxima.util.ExecutorUtils;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.Set;
import java.util.concurrent.Executors;

/**
 * Embeddable gRPC server with guava {@link com.google.common.util.concurrent.Service} interface.
 */
public class GrpcServer extends AbstractIdleService {

  /**
   * gRPC server configuration.
   */
  private final Config config;

  /**
   * Set of gRPC services that will be provided by embedded server.
   */
  private final Set<BindableService> services;

  private Server grpcServer;

  public GrpcServer(Config config, Set<BindableService> services) {
    this.config = config;
    this.services = services;
  }

  @Override
  protected void startUp() throws Exception {
    final ServerBuilder<?> builder = ServerBuilder
        .forPort(config.getInt("port"))
        .executor(Executors.newFixedThreadPool(
            config.getInt("num-threads"),
            ExecutorUtils.daemonThreadFactory("ingest-grpc")));

    services.forEach(builder::addService);

    grpcServer = builder
        .build()
        .start();
  }

  @Override
  protected void shutDown() throws Exception {
    grpcServer.shutdown().awaitTermination();
  }
}

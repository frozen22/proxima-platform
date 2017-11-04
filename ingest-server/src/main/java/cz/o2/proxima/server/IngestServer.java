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

package cz.o2.proxima.server;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.grpc.GrpcServer;
import cz.o2.proxima.server.grpc.IngestService;
import cz.o2.proxima.server.grpc.RequestProcessorImpl;
import cz.o2.proxima.server.grpc.RetrieveService;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.util.ExecutorUtils;
import io.grpc.BindableService;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The ingestion server.
 */
@Slf4j
public class IngestServer {

  /**
   * Run Forrest, run!
   */
  public static void main(String[] args) throws Exception {

    final Config config = args.length == 0
        ? ConfigFactory.load().resolve()
        : ConfigFactory.parseFile(new File(args[0])).resolve();

    // ~ initialize entity repository

    final Repository repository = Repository.of(config);

    if (repository.isEmpty()) {
      throw new IllegalArgumentException("No valid entities were found in the provided config!");
    }

    // ~ initialize callback executor

    final Config callbackExecutorConfig = config.getConfig("ingest.callback-executor");
    final ExecutorService callbackExecutor = Executors.newFixedThreadPool(
        callbackExecutorConfig.getInt("num-threads"),
        ExecutorUtils.daemonThreadFactory("callback-executor"));

    // ~ initialize rpc services

    final Set<BindableService> services = Sets.newHashSet(
        new IngestService(new RequestProcessorImpl(repository), callbackExecutor),
        new RetrieveService(repository));

    final GrpcServer grpcServer = new GrpcServer(config.getConfig("ingest.grpc-server"), services);

    // ~ register metrics

    Metrics.register();

    // ~ start server

    grpcServer.startAsync().awaitRunning();

    // ~ register shutdown hook

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Gracefully shutting down the ingest server.");
      grpcServer.stopAsync();
    }));

    grpcServer.awaitTerminated();

    // ~ gracefully close callback executor

    ExecutorUtils.close(
        callbackExecutor,
        callbackExecutorConfig.getDuration("shutdown-timeout"));

    log.info("Bye, bye!");
  }
}

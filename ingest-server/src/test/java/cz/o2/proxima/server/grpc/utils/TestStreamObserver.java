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
package cz.o2.proxima.server.grpc.utils;

import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class TestStreamObserver<T> implements StreamObserver<T> {

  private final Result<T> result;

  public TestStreamObserver(Result<T> result) {
    this.result = result;
  }

  @Override
  public void onNext(T t) {
    result.getMessages().add(t);
  }

  @Override
  public void onError(Throwable error) {
    result.setError(error);
    result.getDone().countDown();
  }

  @Override
  public void onCompleted() {
    result.getDone().countDown();
  }

  public static class Result<T> {

    @Getter
    @Setter
    private Throwable error;

    @Getter
    private final List<T> messages = new ArrayList<>();

    @Getter
    private final CountDownLatch done = new CountDownLatch(1);
  }
}

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

import cz.o2.proxima.proto.service.Rpc;

public class RequestValidator {

  public static boolean isValid(Rpc.ListRequest request) {
    return nonEmpty(request.getEntity(), request.getKey(), request.getWildcardPrefix());
  }

  public static boolean isValid(Rpc.GetRequest request) {
    return nonEmpty(request.getEntity(), request.getKey(), request.getAttribute());
  }

  public static boolean isValid(Rpc.Ingest request) {
    return nonEmpty(request.getKey(), request.getEntity(), request.getAttribute());
  }

  private static boolean nonEmpty(String... fields) {
    for (String field : fields) {
      if (field.isEmpty()) {
        return false;
      }
    }
    return true;
  }
}

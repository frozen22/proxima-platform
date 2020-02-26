/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.bulk;

import cz.o2.proxima.util.Classpath;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

/** Utility class to be shared between various bulk storages. */
public class Utils {

  /** Retrieve {@link NamingConvention} from configuration. */
  public static NamingConvention getNamingConvention(
      String cfgPrefix, Map<String, Object> cfg, long rollPeriodMs, URI uri) {

    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      digest.update(InetAddress.getLocalHost().getHostName().getBytes(Charset.defaultCharset()));
      String prefix =
          new String(Base64.getEncoder().withoutPadding().encode(digest.digest())).substring(0, 10);
      return Optional.ofNullable(cfg.get(cfgPrefix + "naming-convention"))
          .map(Object::toString)
          .map(cls -> Classpath.newInstance(cls, NamingConvention.class))
          .orElse(
              NamingConvention.defaultConvention(
                  Duration.ofMillis(rollPeriodMs), uri.getPath(), prefix));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  };

  public static FileFormat getFileFormat(String cfgPrefix, Map<String, Object> cfg) {
    String format =
        Optional.ofNullable(cfg.get(cfgPrefix + "format")).map(Object::toString).orElse("binary");
    boolean gzip =
        Optional.ofNullable(cfg.get(cfgPrefix + "gzip"))
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(false);
    if ("binary".equals(format)) {
      return FileFormat.blob(gzip);
    }
    if ("json".equals(format)) {
      return FileFormat.json(gzip);
    }
    throw new IllegalArgumentException("Unknown format " + format);
  }

  private Utils() {}
}

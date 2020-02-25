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

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.util.Pair;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;

/** Interface wrapping generic convention for naming files. */
@Internal
public interface NamingConvention extends Serializable {

  /**
   * Return default {@link NamingConvention} that is used with {@link BinaryBlob} {@link
   * FileFormat}.
   *
   * @param rollTimePeriod time rolling interval in milliseconds.
   * @return default naming convention with specified time roll period
   */
  static NamingConvention defaultConvention(Duration rollTimePeriod) {
    return new DefaultNamingConvention(rollTimePeriod);
  }

  /**
   * Return default {@link NamingConvention} that is used with {@link BinaryBlob} {@link
   * FileFormat}.
   *
   * @param rollTimePeriod time rolling interval in milliseconds.
   * @param basePath base path (e.g. a name of root directory)
   * @param prefix prefix of all names generated
   * @return default naming convention with given settings
   */
  static NamingConvention defaultConvention(
      Duration rollTimePeriod, String basePath, String prefix) {
    return new DefaultNamingConvention(rollTimePeriod, basePath, prefix);
  }

  /**
   * Convert given timestamp to string representing atomic time range of this naming convention.
   *
   * @param ts timestamp to create name for
   * @return String representation of (prefixable) name representing time range
   */
  String nameOf(long ts);

  /**
   * Convert given time range to prefixes, at least one of which must all {@link Path Paths} in
   * given time range have.
   *
   * @param minTs minimal timestamp (inclusive)
   * @param maxTs maximal timestamp (exclusive)
   * @return String representation of name prefix
   */
  Collection<String> prefixesOf(long minTs, long maxTs);

  /**
   * Validate that the given name belongs to given time range.
   *
   * @param name name of the {@link Path}
   * @param minTs minimal timestamp (inclusive)
   * @param maxTs maximal timestamp (exclusive)
   * @return {@code true} if the name belongs to given time range {@code false} otherwise
   */
  boolean isInRange(String name, long minTs, long maxTs);

  /**
   * Parse min and max timestamp from given string.
   *
   * @param name name generated by this convention
   * @return {@link Pair} of min and max timestamps
   */
  Pair<Long, Long> parseMinMaxTimestamp(String name);
}

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
package cz.o2.proxima.direct.core;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import java.util.Optional;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.storage.internal.AbstractDataAccessor;

/**
 * Interface providing various types of data access patterns to storage.
 */
@Internal
public interface DataAccessor extends AbstractDataAccessor {

  /**
   * Retrieve writer (if applicable).
   * @param context the serializable context provided by repository
   * @return optional {@link AttributeWriterBase} of this accessor
   */
  default Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.empty();
  }

  /**
   * Retrieve commit log reader (if applicable).
   * @param context serializable context provided by repository
   * @return optional @{link CommitLogReader} of this accessor
   */
  default Optional<CommitLogReader> getCommitLogReader(Context context) {
    return Optional.empty();
  }

  /**
   * Retrieve random access reader.
   * @param context serializable context provided by repository
   * @return optional {@link RandomAccessReader} of this accessor
   */
  default Optional<RandomAccessReader> getRandomAccessReader(Context context) {
    return Optional.empty();
  }

  /**
   * Retrieve batch log observable.
   * @param context serializable context provided by repository
   * @return optional {@link BatchLogObservable} of this accessor
   */
  default Optional<BatchLogObservable> getBatchLogObservable(Context context) {
    return Optional.empty();
  }

  /**
   * Retrieve cached view of the data.
   * @param context serializable context provided by repository
   * @return optional {@link CachedView} of this accessor
   */
  default Optional<CachedView> getCachedView(Context context) {
    return Optional.empty();
  }

}
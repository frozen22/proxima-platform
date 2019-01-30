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
package cz.o2.proxima.storage.internal;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;

/**
 * Interface for all module data accessor factories to extend.
 * @param <T> the module specific data accessor
 */
@Internal
public interface AbstractDataAccessorFactory<T extends AbstractDataAccessor>
    extends Serializable {

  /**
   * Marker for acceptance of given URI to this factory.
   */
  public static enum Accept {

    /**
     * The URI is accepted.
     */
    ACCEPT,

    /**
     * The URI is accepted, if there is no other factory, that can accept this
     * URI.
     */
    ACCEPT_IF_NEEDED,

    /**
     * The URI is rejected and cannot be handled by this factory.
     */
    REJECT

  }

  /**
   * Check if this factory can create accessors for given URI.
   * @param uri the URI to create accessor for
   * @return acception mark
   */
  Accept accepts(URI uri);

  /**
   * Create the accessor for given URI.
   * @param entity the descriptor of entity to create accessor for
   * @param uri the URI to create accessor for
   * @param cfg optional additional configuration
   * @return the accessor
   */
  T create(EntityDescriptor entity, URI uri, Map<String, Object> cfg);

}

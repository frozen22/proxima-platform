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
package cz.o2.proxima.direct.s3;

import static cz.o2.proxima.direct.blob.BlobPath.normalizePath;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.core.Context;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/** {@link FileSystem} implementation for gs://. */
@Internal
@Slf4j
public class S3FileSystem extends S3Client implements FileSystem {

  private static final long serialVersionUID = 1L;

  private final URI uri;
  private final NamingConvention namingConvention;
  private final Context context;

  S3FileSystem(S3Accessor accessor, Context context) {
    super(accessor.getUri(), accessor.getCfg());
    this.uri = accessor.getUri();
    this.namingConvention = accessor.getNamingConvention();
    this.context = context;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public Stream<Path> list(long minTs, long maxTs) {
    return getBlobsInRange(minTs, maxTs).stream().map(blob -> S3BlobPath.of(context, this, blob));
  }

  @Override
  public Path newPath(long ts) {
    return S3BlobPath.of(
        context, this, normalizePath(getUri().getPath() + namingConvention.nameOf(ts)));
  }

  private List<String> getBlobsInRange(long startStamp, long endStamp) {
    Collection<String> prefixes =
        namingConvention
            .prefixesOf(startStamp, endStamp)
            .stream()
            .map(e -> normalizePath(getUri().getPath() + e))
            .collect(Collectors.toList());
    List<String> ret =
        prefixes
            .stream()
            .flatMap(
                prefix -> {
                  ObjectListing listing = client().listObjects(getBucket(), prefix);
                  return listing
                      .getObjectSummaries()
                      .stream()
                      .map(S3ObjectSummary::getKey)
                      .filter(name -> namingConvention.isInRange(name, startStamp, endStamp))
                      .sorted();
                })
            .collect(Collectors.toList());
    log.debug("Parsed partitions {} for startStamp {}, endStamp {}", ret, startStamp, endStamp);
    return ret;
  }
}
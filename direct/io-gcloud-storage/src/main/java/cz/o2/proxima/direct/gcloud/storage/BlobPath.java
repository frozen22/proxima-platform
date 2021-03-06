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
package cz.o2.proxima.direct.gcloud.storage;

import com.google.cloud.storage.Blob;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.Path;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;

/** A {@link Path} representation of a remote {@link Blob}. */
@Internal
public class BlobPath implements Path {

  private static final long serialVersionUID = 1L;

  public static Path of(FileSystem fs, Blob blob) {
    return new BlobPath(fs, blob);
  }

  private final FileSystem fs;
  private final Blob blob;

  @VisibleForTesting
  BlobPath(FileSystem fs, Blob blob) {
    this.fs = fs;
    this.blob = blob;
  }

  @Override
  public InputStream reader() {
    return Channels.newInputStream(blob.reader());
  }

  @Override
  public OutputStream writer() {
    return Channels.newOutputStream(blob.writer());
  }

  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public void delete() {
    Preconditions.checkState(blob.delete());
  }

  public Blob getBlob() {
    return blob;
  }

  public String getBlobName() {
    return blob.getName();
  }
}

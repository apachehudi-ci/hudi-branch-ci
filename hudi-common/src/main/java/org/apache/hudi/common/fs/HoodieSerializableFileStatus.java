/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.fs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;

/**
 * A serializable wrapper for FileStatus.
 * <p>
 * Hadoop 2.x FileStatus does not implement Serializable and can cause issues. (HUDI-5936)
 * This class is supposed to make sure FileStatus can be safely serialized by wrapping FileStatus
 * with it, and it should be only used when we absolutely need to serialize FileStatus.
 */
public class HoodieSerializableFileStatus extends FileStatus implements Serializable {

  Path path;
  long length;
  boolean isDirectory;
  short blockReplication;
  long blockSize;
  long modificationTime;

  public HoodieSerializableFileStatus(FileStatus status) {
    this(status.getLen(), status.isDirectory(), status.getReplication(), status.getBlockSize(), status.getModificationTime(), status.getPath());
  }

  public HoodieSerializableFileStatus(long length, boolean isdir, int blockReplication, long blocksize, long modificationTime, Path path) {
    this.path = path;
    this.length = length;
    this.isDirectory = isdir;
    this.blockReplication = (short) blockReplication;
    this.blockSize = blocksize;
    this.modificationTime = modificationTime;
  }

  public static HoodieSerializableFileStatus fromFileStatus(FileStatus status) {
    return new HoodieSerializableFileStatus(status);
  }

  public static HoodieSerializableFileStatus[] fromFileStatuses(FileStatus[] statuses) {
    HoodieSerializableFileStatus[] hoodieFileStatuses = new HoodieSerializableFileStatus[statuses.length];

    // using for loop here to make throwing exception easier
    for (int i = 0; i < statuses.length; i++) {
      hoodieFileStatuses[i] = HoodieSerializableFileStatus.fromFileStatus(statuses[i]);
    }
    return hoodieFileStatuses;
  }

  public FileStatus toFileStatus() {
    return new FileStatus(this.length, this.isDirectory, this.blockReplication, this.blockSize, this.modificationTime, this.path);
  }
}

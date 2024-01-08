/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.Option;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public interface HoodieStorage extends Closeable {

  String getScheme();

  OutputStream create(HoodieLocation location, boolean overwrite) throws IOException;

  void createImmutableFileInPath(HoodieLocation location,
                                 Option<byte[]> content) throws IOException;

  InputStream open(HoodieLocation location) throws IOException;

  OutputStream append(HoodieLocation location) throws IOException;

  boolean exists(HoodieLocation location) throws IOException;

  // should throw FileNotFoundException if not found
  HoodieFileStatus getFileInfo(HoodieLocation location) throws IOException;

  boolean createDirectory(HoodieLocation location) throws IOException;

  List<HoodieFileStatus> listDirectEntries(HoodieLocation location) throws IOException;

  List<HoodieFileStatus> listFiles(HoodieLocation location) throws IOException;

  List<HoodieFileStatus> listDirectEntries(HoodieLocation location,
                                           HoodieLocationFilter filter) throws IOException;

  List<HoodieFileStatus> globEntries(HoodieLocation location, HoodieLocationFilter filter)
      throws IOException;

  boolean rename(HoodieLocation oldLocation, HoodieLocation newLocation) throws IOException;

  boolean deleteDirectory(HoodieLocation location) throws IOException;

  boolean deleteFile(HoodieLocation location) throws IOException;

  HoodieLocation makeQualified(HoodieLocation location);

  Object getFileSystem();

  default OutputStream create(HoodieLocation location) throws IOException {
    return create(location, true);
  }

  default boolean createNewFile(HoodieLocation location) throws IOException {
    if (exists(location)) {
      return false;
    } else {
      create(location, false).close();
      return true;
    }
  }

  default List<HoodieFileStatus> listDirectEntries(List<HoodieLocation> locationList) throws IOException {
    List<HoodieFileStatus> result = new ArrayList<>();
    for (HoodieLocation location : locationList) {
      result.addAll(listDirectEntries(location));
    }
    return result;
  }

  default List<HoodieFileStatus> globEntries(HoodieLocation location) throws IOException {
    return globEntries(location, e -> true);
  }
}

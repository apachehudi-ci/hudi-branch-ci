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

package org.apache.hudi.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Statistics about a single Hoodie delta log operation.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("rawtypes")
public class HoodieDeltaWriteStat extends HoodieWriteStat {

  private int logVersion;
  private long logOffset;
  private String baseFile;
  private List<String> logFiles = new ArrayList<>();

  /**
   * Relative delete-log file path (with partition) to its size. A native log append flushes a separate
   * {@code .deletes} file alongside the data file, but only the data file is captured by {@link #getPath()}.
   * Recording the delete file here lets downstream consumers (metadata table file listing, marker reconciliation)
   * account for it, mirroring how {@link HoodieWriteStat#getCdcStats()} tracks CDC files.
   */
  @Getter(lombok.AccessLevel.NONE)
  private Map<String, Long> deleteFileStats;

  public void addLogFiles(String logFile) {
    logFiles.add(logFile);
  }

  public void addDeleteFileStat(String deleteFilePath, long fileSize) {
    if (deleteFileStats == null) {
      deleteFileStats = new HashMap<>();
    }
    deleteFileStats.put(deleteFilePath, fileSize);
  }

  @Nullable
  public Map<String, Long> getDeleteFileStats() {
    return deleteFileStats;
  }

  /**
   * Make a new write status and copy basic fields from current object
   * @return copy write status
   */
  public HoodieDeltaWriteStat copy() {
    HoodieDeltaWriteStat copy = new HoodieDeltaWriteStat();
    copy.setFileId(getFileId());
    copy.setPartitionPath(getPartitionPath());
    copy.setPrevCommit(getPrevCommit());
    copy.setBaseFile(getBaseFile());
    copy.setLogFiles(new ArrayList<>(getLogFiles()));
    if (deleteFileStats != null) {
      copy.setDeleteFileStats(new HashMap<>(deleteFileStats));
    }
    return copy;
  }
}

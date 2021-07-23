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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.execution.bulkinsert.NonSortPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkOptimizeWriteCommitActionExecutor<T extends HoodieRecordPayload<T>> extends BaseSparkCommitActionExecutor<T> {
  private final JavaRDD<HoodieRecord> inputRecordsRDD;

  public SparkOptimizeWriteCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                String instantTime, JavaRDD<HoodieRecord> inputRecordsRDD) {
    this(context, config, table, instantTime, inputRecordsRDD, Option.empty());
  }

  public SparkOptimizeWriteCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                String instantTime, JavaRDD<HoodieRecord> inputRecordsRDD, Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, WriteOperationType.OPTIMIZE, extraMetadata);
    this.inputRecordsRDD = inputRecordsRDD;

  }

  @Override
  public HoodieWriteMetadata<JavaRDD> execute() {
    try {
      return SparkBulkInsertHelper.newInstance().bulkInsert(inputRecordsRDD, instantTime, table, config,
          this, false, Option.of(new NonSortPartitioner()));
    } catch (HoodieInsertException ie) {
      throw ie;
    } catch (Throwable e) {
      throw new HoodieInsertException("Failed to optimize data layout for commit time " + instantTime, e);
    }
  }

  @Override
  protected String getCommitActionType() {
    return HoodieTimeline.REPLACE_COMMIT_ACTION;
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieWriteMetadata<JavaRDD<WriteStatus>> writeStatuses) {
    return writeStatuses.getWriteStatuses().map(status -> status.getStat().getPartitionPath()).distinct().mapToPair(partitionPath ->
        new Tuple2<>(partitionPath, getAllExistingFileIds(partitionPath))).collectAsMap();
  }

  protected List<String> getAllExistingFileIds(String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(fg -> fg.getFileId()).distinct().collect(Collectors.toList());
  }
}



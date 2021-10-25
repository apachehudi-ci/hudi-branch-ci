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

package org.apache.hudi.index.simple;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.index.IndexDelegate;
import org.apache.hudi.table.HoodieBaseTable;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.stream.Stream;

import static org.apache.hudi.client.utils.SparkDataFrameWriterUtils.HOODIE_META_COLS;
import static org.apache.hudi.client.utils.SparkDataFrameWriterUtils.HOODIE_META_COL_NAMES;
import static org.apache.hudi.client.utils.SparkDataFrameWriterUtils.JOIN_COLS;
import static org.apache.hudi.client.utils.SparkDataFrameWriterUtils.JOIN_COL_NAMES_SEQ;
import static org.apache.hudi.client.utils.SparkDataFrameWriterUtils.addHoodieKeyPartitionCols;
import static org.apache.hudi.client.utils.SparkDataFrameWriterUtils.getHoodieOptions;
import static org.apache.spark.sql.functions.broadcast;

public class SparkHoodieBroadcastSimpleIndex implements IndexDelegate<Dataset<Row>> {

  @Override
  public Dataset<Row> tagLocation(Dataset<Row> upserts, HoodieEngineContext context, HoodieBaseTable table) {
    final Column[] orderedAllCols = Stream
        .concat(Stream.of(HOODIE_META_COL_NAMES), Stream.of(upserts.schema().fieldNames()))
        .map(Column::new).toArray(Column[]::new);

    // broadcast incoming records to join with all existing records
    Dataset<Row> hoodieKeyedUpserts = addHoodieKeyPartitionCols(upserts);
    Dataset<Row> allRecords = ((HoodieSparkEngineContext) context).getSqlContext().read().format("hudi")
        .options(getHoodieOptions(table.getConfig())).load(table.getMetaClient().getBasePath());
    Dataset<Row> taggedHoodieMetaColsUpdates = allRecords.select(HOODIE_META_COLS)
        .join(broadcast(hoodieKeyedUpserts.select(JOIN_COLS)), JOIN_COL_NAMES_SEQ);

    // join tagged records back to incoming records
    Dataset<Row> taggedHoodieUpserts = hoodieKeyedUpserts
        .join(taggedHoodieMetaColsUpdates, JOIN_COL_NAMES_SEQ, "left").select(orderedAllCols);

    return taggedHoodieUpserts;
  }

  public boolean rollbackCommit(String commitTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return true;
  }

  public boolean canIndexLogFiles() {
    return false;
  }

  public boolean isImplicitWithStorage() {
    return true;
  }

}

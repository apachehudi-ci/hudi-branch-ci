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

package org.apache.hudi.functional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end null ordering (precombine) value behavior on the 1.x FileGroupReader merge path, for
 * COW and MOR tables and the AVRO and SPARK record types.
 *
 * <p>Ordering values are preserved as null (never coerced to a sentinel). On an event-time merge, a
 * null <em>base</em> ordering value ranks lowest so a real incoming record wins; a null
 * <em>incoming</em> ordering value is invalid and fails (rejected at write time for AVRO, surfaced as
 * a NullPointerException on the comparison for SPARK). Commit-time ordering does not compare ordering
 * values, so nulls pass through and the incoming record wins.
 */
class TestNullOrderingValueMerge {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark = SparkSession.builder()
        .appName("null-ordering-merge-1x")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static final StructType SCHEMA = new StructType()
      .add("id", DataTypes.StringType, false)
      .add("part", DataTypes.StringType, false)
      .add("ts", DataTypes.LongType, true)
      .add("value", DataTypes.StringType, false);

  static Stream<Arguments> cases() {
    // record type x table type x merge mode x null-case.
    List<Arguments> args = new ArrayList<>();
    for (String recordType : new String[] {"AVRO", "SPARK"}) {
      for (String tableType : new String[] {"COPY_ON_WRITE", "MERGE_ON_READ"}) {
        for (String mergeMode : new String[] {"EVENT_TIME_ORDERING", "COMMIT_TIME_ORDERING"}) {
          args.add(Arguments.of(recordType, tableType, mergeMode, "base-null", null, 100L));
          args.add(Arguments.of(recordType, tableType, mergeMode, "incoming-null", 100L, null));
          args.add(Arguments.of(recordType, tableType, mergeMode, "both-null", null, null));
        }
      }
    }
    return args.stream();
  }

  @ParameterizedTest(name = "{0} / {1} / {2} / {3}")
  @MethodSource("cases")
  void nullOrderingValueMerge(String recordType, String tableType, String mergeMode, String caseName,
                              Long baseTs, Long incomingTs, @TempDir Path tmp) {
    // Only a null base ordering value against a real incoming value succeeds (incoming wins). An
    // event-time upsert whose incoming ordering value is null fails. Commit-time ordering always
    // succeeds because it does not compare ordering values.
    boolean expectSuccess = "COMMIT_TIME_ORDERING".equals(mergeMode) || incomingTs != null;
    String path = tmp.resolve(recordType + "_" + tableType + "_" + mergeMode + "_" + caseName).toString();

    writeRow(path, recordType, tableType, mergeMode, "insert", SaveMode.Overwrite, baseTs, "base");

    if (!expectSuccess) {
      // The failure surfaces at the combining write (AVRO reject, and COW where the merge runs at
      // write time) or on the read merge (MOR for the SPARK record type), so wrap both.
      Throwable thrown = assertThrows(Exception.class, () -> {
        writeRow(path, recordType, tableType, mergeMode, "upsert", SaveMode.Append, incomingTs, "incoming");
        readRows(path);
      });
      Throwable root = rootCause(thrown);
      boolean writeReject = root instanceof IllegalArgumentException
          && root.getMessage() != null && root.getMessage().contains("has null value for record key");
      assertTrue(writeReject || root instanceof NullPointerException,
          "expected a null-ordering write rejection or comparison failure, got: " + root);
      return;
    }

    writeRow(path, recordType, tableType, mergeMode, "upsert", SaveMode.Append, incomingTs, "incoming");
    List<Row> rows = readRows(path);
    assertEquals(1, rows.size(), "expected exactly one record for key k1");
    Row row = rows.get(0);
    assertEquals("incoming", row.getAs("value"));
    int tsIdx = row.fieldIndex("ts");
    if (incomingTs == null) {
      assertTrue(row.isNullAt(tsIdx), "ts should remain NULL, the sentinel must not be materialized");
    } else {
      assertEquals(incomingTs.longValue(), row.getLong(tsIdx));
    }
  }

  private List<Row> readRows(String path) {
    return spark.read().format("hudi").load(path)
        .select("id", "ts", "value").where("id = 'k1'").collectAsList();
  }

  private static Throwable rootCause(Throwable t) {
    while (t.getCause() != null && t.getCause() != t) {
      t = t.getCause();
    }
    return t;
  }

  private void writeRow(String path, String recordType, String tableType, String mergeMode, String operation,
                        SaveMode mode, Long ts, String value) {
    Dataset<Row> df = spark.createDataFrame(
        Arrays.asList(RowFactory.create("k1", "p1", ts, value)), SCHEMA);
    Map<String, String> opts = new HashMap<>();
    opts.put("hoodie.table.name", "null_ordering_t");
    opts.put("hoodie.datasource.write.recordkey.field", "id");
    opts.put("hoodie.datasource.write.partitionpath.field", "part");
    opts.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator");
    opts.put("hoodie.datasource.write.table.type", tableType);
    opts.put("hoodie.datasource.write.hive_style_partitioning", "true");
    opts.put("hoodie.metadata.enable", "false");
    opts.put("hoodie.compact.inline", "false");
    opts.put("hoodie.record.merge.mode", mergeMode);
    opts.put("hoodie.datasource.write.operation", operation);
    // Force the record type via the record merger: default (unset) -> AVRO; DefaultSparkRecordMerger -> SPARK.
    if ("SPARK".equals(recordType)) {
      opts.put("hoodie.write.record.merge.custom.implementation.classes", "org.apache.hudi.DefaultSparkRecordMerger");
    }
    // Commit-time ordering does not use a precombine field; setting one makes the writer infer
    // event-time ordering and then reject the explicit commit-time merge mode.
    if ("EVENT_TIME_ORDERING".equals(mergeMode)) {
      opts.put("hoodie.datasource.write.precombine.field", "ts");
    }
    df.write().format("hudi").options(opts).mode(mode).save(path);
  }
}

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

package org.apache.hudi.util;

import org.apache.hudi.HoodieInternalRowUtils;
import org.apache.hudi.commmon.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.RowKeyGeneratorHelper;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import scala.Tuple2;

public class HoodieSparkRecordUtils {

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(InternalRow data, StructType structType) {
    return new HoodieSparkRecord(data, structType);
  }

  /**
   * Utility method to convert InternalRow to HoodieRecord using schema and payload class.
   */
  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, InternalRow data, String preCombineField, boolean withOperationField) {
    return convertToHoodieSparkRecord(structType, data, preCombineField,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, Option.empty());
  }

  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, InternalRow data, String preCombineField, boolean withOperationField,
      Option<String> partitionName) {
    return convertToHoodieSparkRecord(structType, data, preCombineField,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, partitionName);
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, InternalRow data, String preCombineField, Pair<String, String> recordKeyPartitionPathFieldPair,
      boolean withOperationField, Option<String> partitionName) {
    final String recKey = getValue(structType, recordKeyPartitionPathFieldPair.getKey(), data).toString();
    final String partitionPath = (partitionName.isPresent() ? partitionName.get() :
        getValue(structType, recordKeyPartitionPathFieldPair.getRight(), data).toString());

    Object preCombineVal = getPreCombineVal(structType, data, preCombineField);
    HoodieOperation operation = withOperationField
        ? HoodieOperation.fromName(getNullableValAsString(structType, data, HoodieRecord.OPERATION_METADATA_FIELD)) : null;
    return new HoodieSparkRecord(new HoodieKey(recKey, partitionPath), data, structType, operation, (Comparable<?>) preCombineVal);
  }

  /**
   * Returns the preCombine value with given field name.
   *
   * @param data            The avro record
   * @param preCombineField The preCombine field name
   * @return the preCombine field value or 0 if the field does not exist in the avro schema
   */
  private static Object getPreCombineVal(StructType structType, InternalRow data, String preCombineField) {
    if (preCombineField == null) {
      return 0;
    }
    return !HoodieInternalRowUtils.getCachedSchemaPosMap(structType).contains(preCombineField)
        ? 0 : getValue(structType, preCombineField, data);
  }

  private static Object getValue(StructType structType, String fieldName, InternalRow row) {
    Tuple2<StructField, Object> tuple2 = HoodieInternalRowUtils.getCachedSchemaPosMap(structType).apply(fieldName);
    int pos = (Integer) tuple2._2;
    DataType type = tuple2._1.dataType();
    return row.get(pos, type);
  }

  /**
   * Returns the string value of the given record {@code rec} and field {@code fieldName}. The field and value both could be missing.
   *
   * @param row       The record
   * @param fieldName The field name
   * @return the string form of the field or empty if the schema does not contain the field name or the value is null
   */
  private static Option<String> getNullableValAsString(StructType structType, InternalRow row, String fieldName) {
    String fieldVal = !HoodieInternalRowUtils.getCachedSchemaPosMap(structType).contains(fieldName)
        ? null : StringUtils.objToString(getValue(structType, fieldName, row));
    return Option.ofNullable(fieldVal);
  }

  /**
   * Gets record column values into one object.
   *
   * @param record  Hoodie record.
   * @param columns Names of the columns to get values.
   * @param structType  {@link StructType} instance.
   * @return Column value if a single column, or concatenated String values by comma.
   */
  public static Object getRecordColumnValues(HoodieSparkRecord record,
      String[] columns,
      StructType structType, boolean consistentLogicalTimestampEnabled) {
    InternalRow row = record.getData();
    if (columns.length == 1) {
      List<Integer> posList = RowKeyGeneratorHelper.getFieldSchemaInfo(structType, columns[0], false).getKey();
      return RowKeyGeneratorHelper.getNestedFieldVal(row, structType, posList, true);
    } else {
      // TODO this is inefficient, instead we can simply return array of Comparable
      StringBuilder sb = new StringBuilder();
      for (String col : columns) {
        // TODO support consistentLogicalTimestampEnabled
        List<Integer> posList = RowKeyGeneratorHelper.getFieldSchemaInfo(structType, col, false).getKey();
        sb.append(RowKeyGeneratorHelper.getNestedFieldValAsString(row, structType, posList, true));
      }
      return sb.toString();
    }
  }
}

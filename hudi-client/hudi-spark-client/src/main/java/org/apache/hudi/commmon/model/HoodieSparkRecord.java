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

package org.apache.hudi.commmon.model;

import org.apache.hudi.HoodieInternalRowUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.SparkKeyGeneratorInterface;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.util.HoodieSparkRecordUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import scala.Tuple2;

import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Spark Engine-specific Implementations of `HoodieRecord`.
 */
public class HoodieSparkRecord extends HoodieRecord<InternalRow> {

  // IndexedRecord hold its schema, InternalRow should also hold its schema
  private final StructType structType;

  public HoodieSparkRecord(InternalRow data, StructType schema) {
    super(null, data);
    this.structType = schema;
  }

  public HoodieSparkRecord(InternalRow data, StructType schema, Comparable orderingVal) {
    super(null, data, orderingVal);
    this.structType = schema;
  }

  public HoodieSparkRecord(HoodieKey key, InternalRow data, StructType schema) {
    super(key, data);
    this.structType = schema;
  }

  public HoodieSparkRecord(HoodieKey key, InternalRow data, StructType schema, Comparable orderingVal) {
    super(key, data, orderingVal);
    this.structType = schema;
  }

  public HoodieSparkRecord(HoodieKey key, InternalRow data, StructType schema, HoodieOperation operation, Comparable orderingVal) {
    super(key, data, operation, orderingVal);
    this.structType = schema;
  }

  public HoodieSparkRecord(HoodieSparkRecord record) {
    super(record);
    this.structType = record.structType;
  }

  @Override
  public HoodieRecord<InternalRow> newInstance() {
    return new HoodieSparkRecord(this);
  }

  @Override
  public HoodieRecord<InternalRow> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieSparkRecord(key, data, structType, op, getOrderingValue());
  }

  @Override
  public HoodieRecord<InternalRow> newInstance(HoodieKey key) {
    return new HoodieSparkRecord(key, data, structType, getOrderingValue());
  }

  @Override
  public void deflate() {
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (key != null) {
      return getRecordKey();
    }
    return keyGeneratorOpt.isPresent() ? ((SparkKeyGeneratorInterface) keyGeneratorOpt.get()).getRecordKey(data, structType) : data.getString(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal());
  }

  @Override
  public String getRecordKey(String keyFieldName) {
    if (key != null) {
      return getRecordKey();
    }
    Tuple2<StructField, Object> tuple2 = HoodieInternalRowUtils.getCachedSchemaPosMap(structType).get(keyFieldName).get();
    DataType dataType = tuple2._1.dataType();
    int pos = (Integer) tuple2._2;
    return data.get(pos, dataType).toString();
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }

  @Override
  public Object getRecordColumnValues(String[] columns, Schema schema, boolean consistentLogicalTimestampEnabled) {
    return HoodieSparkRecordUtils.getRecordColumnValues(this, columns, structType, consistentLogicalTimestampEnabled);
  }

  @Override
  public HoodieRecord mergeWith(Schema schema, HoodieRecord other, Schema otherSchema, Schema writerSchema) throws IOException {
    StructType otherStructType = HoodieInternalRowUtils.getCachedSchema(otherSchema);
    StructType writerStructType = HoodieInternalRowUtils.getCachedSchema(writerSchema);
    InternalRow mergeRow = HoodieInternalRowUtils.stitchRecords(data, structType, (InternalRow) other.getData(), otherStructType, writerStructType);
    return new HoodieSparkRecord(getKey(), mergeRow, writerStructType, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Schema targetSchema, TypedProperties props) throws IOException {
    StructType targetStructType = HoodieInternalRowUtils.getCachedSchema(targetSchema);
    InternalRow rewriteRow = HoodieInternalRowUtils.rewriteRecord(data, structType, targetStructType);
    return new HoodieSparkRecord(getKey(), rewriteRow, targetStructType, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties prop, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields) throws IOException {
    StructType writeSchemaWithMetaFieldsStructType = HoodieInternalRowUtils.getCachedSchema(writeSchemaWithMetaFields);
    InternalRow rewriteRow = schemaOnReadEnabled ? HoodieInternalRowUtils.rewriteRecordWithNewSchema(data, structType, writeSchemaWithMetaFieldsStructType, new HashMap<>())
        : HoodieInternalRowUtils.rewriteRecord(data, structType, writeSchemaWithMetaFieldsStructType);
    return new HoodieSparkRecord(getKey(), rewriteRow, writeSchemaWithMetaFieldsStructType, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithMetadata(Schema recordSchema, Properties prop, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields, String fileName) throws IOException {
    StructType writeSchemaWithMetaFieldsStructType = HoodieInternalRowUtils.getCachedSchema(writeSchemaWithMetaFields);
    InternalRow rewriteRow = schemaOnReadEnabled ? HoodieInternalRowUtils.rewriteEvolutionRecordWithMetadata(data, structType, writeSchemaWithMetaFieldsStructType, fileName)
        : HoodieInternalRowUtils.rewriteRecordWithMetadata(data, structType, writeSchemaWithMetaFieldsStructType, fileName);
    return new HoodieSparkRecord(getKey(), rewriteRow, writeSchemaWithMetaFieldsStructType, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema, Map<String, String> renameCols) throws IOException {
    StructType newStructType = HoodieInternalRowUtils.getCachedSchema(newSchema);
    InternalRow rewriteRow = HoodieInternalRowUtils.rewriteRecordWithNewSchema(data, structType, newStructType, renameCols);
    return new HoodieSparkRecord(getKey(), rewriteRow, newStructType, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema) throws IOException {
    StructType newStructType = HoodieInternalRowUtils.getCachedSchema(newSchema);
    InternalRow rewriteRow = HoodieInternalRowUtils.rewriteRecord(data, structType, newStructType);
    return new HoodieSparkRecord(getKey(), rewriteRow, structType, getOperation());
  }

  @Override
  public HoodieRecord overrideMetadataFieldValue(Schema recordSchema, Properties prop, int pos, String newValue) throws IOException {
    data.update(pos, CatalystTypeConverters.convertToCatalyst(newValue));
    return this;
  }

  @Override
  public HoodieRecord addMetadataValues(Schema recordSchema, Properties prop, Map<HoodieMetadataField, String> metadataValues) throws IOException {
    Arrays.stream(HoodieMetadataField.values()).forEach(metadataField -> {
      String value = metadataValues.get(metadataField);
      if (value != null) {
        data.update(recordSchema.getField(metadataField.getFieldName()).pos(), CatalystTypeConverters.convertToCatalyst(value));
      }
    });
    return this;
  }

  @Override
  public HoodieRecord expansion(Schema schema, Properties prop, String payloadClass,
      String preCombineField,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Option<Boolean> populateMetaFieldsOp) {
    boolean populateMetaFields = populateMetaFieldsOp.orElse(false);
    if (populateMetaFields) {
      return HoodieSparkRecordUtils.convertToHoodieSparkRecord(structType, data, preCombineField, withOperation);
    } else if (simpleKeyGenFieldsOpt.isPresent()) {
      return HoodieSparkRecordUtils.convertToHoodieSparkRecord(structType, data, preCombineField, simpleKeyGenFieldsOpt.get(), withOperation, Option.empty());
    } else {
      return HoodieSparkRecordUtils.convertToHoodieSparkRecord(structType, data, preCombineField, withOperation, partitionNameOp);
    }
  }

  @Override
  public HoodieRecord transform(Schema schema, Properties prop, boolean useKeygen) {
    StructType structType = HoodieInternalRowUtils.getCachedSchema(schema);
    Option<SparkKeyGeneratorInterface> keyGeneratorOpt = Option.empty();
    if (useKeygen && !Boolean.parseBoolean(prop.getOrDefault(POPULATE_META_FIELDS.key(), POPULATE_META_FIELDS.defaultValue().toString()).toString())) {
      try {
        keyGeneratorOpt = Option.of((SparkKeyGeneratorInterface) HoodieSparkKeyGeneratorFactory.createKeyGenerator(new TypedProperties(prop)));
      } catch (IOException e) {
        throw new HoodieException("Only SparkKeyGeneratorInterface are supported when meta columns are disabled ", e);
      }
    }
    String key = keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getRecordKey(data, structType)
        : data.get(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal(), StringType).toString();
    String partition = keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getPartitionPath(data, structType)
        : data.get(HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.ordinal(), StringType).toString();
    this.key = new HoodieKey(key, partition);

    return this;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  @Override
  public boolean isPresent(Schema schema, Properties prop) throws IOException {
    if (null == data) {
      return false;
    }
    if (schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD) == null) {
      return true;
    }
    Object deleteMarker = data.get(schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).pos(), BooleanType);
    return !(deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }

  @Override
  public boolean shouldIgnore(Schema schema, Properties prop) throws IOException {
    // TODO SENTINEL should refactor SENTINEL without Avro(GenericRecord)
    if (null != data && data.equals(SENTINEL)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Option<IndexedRecord> toIndexedRecord(Schema schema, Properties prop) throws IOException {
    return Option.of(HoodieInternalRowUtils.row2Avro(schema, data));
  }
}

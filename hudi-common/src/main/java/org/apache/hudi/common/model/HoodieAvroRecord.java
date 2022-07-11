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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.TypeUtils.unsafeCast;

public class HoodieAvroRecord<T extends HoodieRecordPayload> extends HoodieRecord<T> {
  public HoodieAvroRecord(HoodieKey key, T data) {
    super(key, data);
  }

  public HoodieAvroRecord(HoodieKey key, T data, HoodieOperation operation) {
    super(key, data, operation, null);
  }

  public HoodieAvroRecord(HoodieRecord<T> record) {
    super(record);
  }

  public HoodieAvroRecord() {
  }

  @Override
  public HoodieRecord<T> newInstance() {
    return new HoodieAvroRecord<>(this);
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieAvroRecord<>(key, data, op);
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key) {
    return new HoodieAvroRecord<>(key, data);
  }

  @Override
  public T getData() {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }

  @Override
  public Comparable getOrderingValue() {
    return this.getData().getOrderingValue();
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    return getRecordKey();
  }

  @Override
  public String getRecordKey(String keyFieldName) {
    return getRecordKey();
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  @Override
  public Object getRecordColumnValues(String[] columns, Schema schema, boolean consistentLogicalTimestampEnabled) {
    return HoodieAvroUtils.getRecordColumnValues(this, columns, schema, consistentLogicalTimestampEnabled);
  }

  @Override
  public Option<IndexedRecord> toIndexedRecord(Schema schema, Properties props) throws IOException {
    return getData().getInsertValue(schema, props);
  }

  @Override
  public HoodieRecord mergeWith(Schema schema, HoodieRecord other, Schema otherSchema, Schema writerSchema) throws IOException {
    ValidationUtils.checkState(other instanceof HoodieAvroRecord);
    GenericRecord mergedPayload = HoodieAvroUtils.stitchRecords(
        (GenericRecord) toIndexedRecord(schema, new Properties()).get(),
        (GenericRecord) other.toIndexedRecord(otherSchema, new Properties()).get(),
        writerSchema);
    return new HoodieAvroRecord(getKey(), instantiateRecordPayloadWrapper(mergedPayload, getOrderingValue()), getOperation());
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Schema targetSchema) throws IOException {
    Option<IndexedRecord> avroRecordPayloadOpt = getData().getInsertValue(recordSchema, new Properties());
    GenericRecord avroPayloadInNewSchema =
        HoodieAvroUtils.rewriteRecord((GenericRecord) avroRecordPayloadOpt.get(), targetSchema);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(avroPayloadInNewSchema), getOperation());
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties props, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields) throws IOException {
    GenericRecord record = (GenericRecord) getData().getInsertValue(recordSchema, props).get();
    GenericRecord rewriteRecord = schemaOnReadEnabled ? HoodieAvroUtils.rewriteRecordWithNewSchema(record, writeSchemaWithMetaFields, new HashMap<>())
        : HoodieAvroUtils.rewriteRecord(record, writeSchemaWithMetaFields);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(rewriteRecord), getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithMetadata(Schema recordSchema, Properties props, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields, String fileName) throws IOException {
    GenericRecord record = (GenericRecord) getData().getInsertValue(recordSchema, props).get();
    GenericRecord rewriteRecord =  schemaOnReadEnabled ? HoodieAvroUtils.rewriteEvolutionRecordWithMetadata(record, writeSchemaWithMetaFields, fileName)
        : HoodieAvroUtils.rewriteRecordWithMetadata(record, writeSchemaWithMetaFields, fileName);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(rewriteRecord), getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException {
    GenericRecord oldRecord = (GenericRecord) getData().getInsertValue(recordSchema, props).get();
    GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(oldRecord, newSchema, renameCols);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(rewriteRecord), getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema) throws IOException {
    GenericRecord oldRecord = (GenericRecord) getData().getInsertValue(recordSchema, props).get();
    GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecord(oldRecord, newSchema);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(rewriteRecord), getOperation());
  }

  @Override
  public HoodieRecord overrideMetadataFieldValue(Schema recordSchema, Properties props, int pos, String newValue) throws IOException {
    IndexedRecord record = (IndexedRecord) data.getInsertValue(recordSchema, props).get();
    record.put(pos, newValue);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload((GenericRecord) record), getOperation());
  }

  @Override
  public HoodieRecord addMetadataValues(Schema recordSchema, Properties props, Map<HoodieMetadataField, String> metadataValues) throws IOException {
    // NOTE: RewriteAvroPayload is expected here
    GenericRecord avroRecordPayload = (GenericRecord) getData().getInsertValue(recordSchema, props).get();

    Arrays.stream(HoodieMetadataField.values()).forEach(metadataField -> {
      String value = metadataValues.get(metadataField);
      if (value != null) {
        avroRecordPayload.put(metadataField.getFieldName(), value);
      }
    });

    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(avroRecordPayload), getOperation());
  }

  @Override
  public HoodieRecord expansion(
      Schema schema,
      Properties props,
      String payloadClass,
      String preCombineField,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Option<Boolean> populateMetaFieldsOp) throws IOException {
    IndexedRecord value = (IndexedRecord) data.getInsertValue(schema, props).get();
    return HoodieAvroUtils.createHoodieRecordFromAvro(value, payloadClass, preCombineField, simpleKeyGenFieldsOpt, withOperation, partitionNameOp, populateMetaFieldsOp);
  }

  @Override
  public HoodieRecord transform(Schema schema, Properties props, boolean useKeygen) {
    throw new UnsupportedOperationException();
  }

  public Option<Map<String, String>> getMetadata() {
    return getData().getMetadata();
  }

  @Override
  public boolean isPresent(Schema schema, Properties props) throws IOException {
    return getData().getInsertValue(schema, props).isPresent();
  }

  @Override
  public boolean shouldIgnore(Schema schema, Properties props) throws IOException {
    Option<IndexedRecord> insertRecord = getData().getInsertValue(schema, props);
    // just skip the ignored record
    if (insertRecord.isPresent() && insertRecord.get().equals(SENTINEL)) {
      return true;
    } else {
      return false;
    }
  }

  @Nonnull
  private T instantiateRecordPayloadWrapper(Object combinedAvroPayload, Comparable newPreCombineVal) {
    return unsafeCast(
        HoodieRecordUtils.loadPayload(
            getData().getClass().getCanonicalName(),
            new Object[]{combinedAvroPayload, newPreCombineVal},
            GenericRecord.class,
            Comparable.class));
  }
}

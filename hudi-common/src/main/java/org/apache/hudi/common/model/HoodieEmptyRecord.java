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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HoodieEmptyRecord<T> extends HoodieRecord<T> {

  private final HoodieRecordType type;

  public HoodieEmptyRecord(HoodieKey key, HoodieRecordType type) {
    super(key, null);
    this.type = type;
  }

  public HoodieEmptyRecord(HoodieKey key, Comparable<?> orderingVal, HoodieRecordType type) {
    super(key, null, orderingVal);
    this.type = type;
  }

  public HoodieEmptyRecord(HoodieKey key, HoodieOperation operation, Comparable<?> orderingVal, HoodieRecordType type) {
    super(key, null, operation, orderingVal);
    this.type = type;
  }

  public HoodieEmptyRecord(HoodieRecord<T> record, HoodieRecordType type) {
    super(record);
    this.type = type;
  }

  public HoodieEmptyRecord(HoodieRecordType type) {
    this.type = type;
  }

  @Override
  public HoodieRecord<T> newInstance() {
    return this;
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieEmptyRecord<>(key, op, type);
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key) {
    return new HoodieEmptyRecord<>(key, type);
  }

  @Override
  public HoodieRecordType getRecordType() {
    return type;
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    return key.getRecordKey();
  }

  @Override
  public String getRecordKey(String keyFieldName) {
    return key.getRecordKey();
  }

  @Override
  public Object getRecordColumnValues(String[] columns, Schema schema, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord mergeWith(Schema schema, HoodieRecord other, Schema otherSchema, Schema writerSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Schema targetSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties props, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord rewriteRecordWithMetadata(Schema recordSchema, Properties props, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields, String fileName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord overrideMetadataFieldValue(Schema recordSchema, Properties props, int pos, String newValue) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord addMetadataValues(Schema recordSchema, Properties props, Map<HoodieMetadataField, String> metadataValues) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  @Override
  public boolean isPresent(Schema schema, Properties props) throws IOException {
    return false;
  }

  @Override
  public boolean shouldIgnore(Schema schema, Properties props) throws IOException {
    return false;
  }

  @Override
  public HoodieRecord expansion(Schema schema, Properties props, String payloadClass, String preCombineField, Option<Pair<String, String>> simpleKeyGenFieldsOpt, Boolean withOperation,
      Option<String> partitionNameOp, Option<Boolean> populateMetaFieldsOp) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord transform(Schema schema, Properties props, boolean useKeyGen) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Option<IndexedRecord> toIndexedRecord(Schema schema, Properties props) throws IOException {
    return Option.empty();
  }
}

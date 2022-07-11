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

package org.apache.hudi;

import org.apache.hudi.commmon.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.keygen.RowKeyGeneratorHelper;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class HoodieSparkDefaultRecordMerge extends HoodieSparkRecordMerge {

  @Override
  public Option<HoodieRecord> combineAndGetUpdateValue(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecordType.SPARK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecordType.SPARK);

    // Null check is needed here to support schema evolution. The record in storage may be from old schema where
    // the new ordering column might not be present and hence returns null.
    if (!needUpdatingPersistedRecord(older, newer, props)) {
      return Option.of(older);
    }

    if (newer instanceof HoodieEmptyRecord) {
      return Option.empty();
    } else {
      return Option.of(newer);
    }
  }

  protected boolean needUpdatingPersistedRecord(HoodieRecord older, HoodieRecord newer, Properties properties) {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecordType.SPARK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecordType.SPARK);

    /*
     * Combining strategy here returns currentRecord on disk if incoming record is older.
     * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
     * or an insert/update record. In any case, if it is older than the record in disk, the currentRecord
     * in disk is returned (to be rewritten with new commit time).
     *
     * NOTE: Deletes sent via EmptyHoodieRecordPayload and/or Delete operation type do not hit this code path
     * and need to be dealt with separately.
     */
    Comparable persistedOrderingVal = getOrderingVal((HoodieSparkRecord) older, properties);
    Comparable incomingOrderingVal = getOrderingVal((HoodieSparkRecord) newer, properties);
    return persistedOrderingVal == null || incomingOrderingVal == null || persistedOrderingVal.compareTo(incomingOrderingVal) <= 0;
  }

  private Comparable getOrderingVal(HoodieSparkRecord record, Properties properties) {
    String orderField = properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
    if (orderField != null) {
      List<Integer> posList = RowKeyGeneratorHelper.getFieldSchemaInfo(record.getStructType(), orderField, false).getKey();
      return (Comparable) RowKeyGeneratorHelper.getNestedFieldVal(record.getData(), record.getStructType(), posList, false);
    } else {
      return null;
    }
  }
}

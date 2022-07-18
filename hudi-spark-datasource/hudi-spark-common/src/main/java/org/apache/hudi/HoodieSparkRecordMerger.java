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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieRecord.Source.BASE;
import static org.apache.hudi.common.model.HoodieRecord.Source.LOG;
import static org.apache.hudi.common.model.HoodieRecord.Source.WRITE;

public class HoodieSparkRecordMerger implements HoodieRecordMerger {

  @Override
  public Option<HoodieRecord> merge(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecordType.SPARK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecordType.SPARK);
    if (older.getSource() == BASE && newer.getSource() == LOG) {
      return combineAndGetUpdateValue(older, newer, schema, props);
    } else if (older.getSource() == LOG && newer.getSource() == LOG) {
      return Option.of(preCombine(older, newer, props));
    } else if (older.getSource() == WRITE && newer.getSource() == WRITE) {
      return Option.of(preCombine(older, newer, props));
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }

  private HoodieRecord preCombine(HoodieRecord older, HoodieRecord newer, Properties props) {
    if (older.getData() == null) {
      // use natural order for delete record
      return newer;
    }
    if (older.getOrderingValue(props).compareTo(newer.getOrderingValue(props)) > 0) {
      return older;
    } else {
      return newer;
    }
  }

  protected Option<HoodieRecord> combineAndGetUpdateValue(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
    if (newer.getData() == null) {
      return Option.empty();
    } else {
      return Option.of(newer);
    }
  }
}
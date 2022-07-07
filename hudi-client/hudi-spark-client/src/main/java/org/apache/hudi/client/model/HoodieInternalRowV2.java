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

package org.apache.hudi.client.model;

import org.apache.hudi.common.model.HoodieRecord;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Internal Row implementation for Hoodie Row. It wraps an {@link InternalRow} and keeps meta columns locally. But the {@link InternalRow} does *not* include the meta columns.
 */
public class HoodieInternalRowV2 extends HoodieInternalRow {

  public HoodieInternalRowV2(InternalRow row, boolean hasOperation) {
    super(null, null, null, null, null, row);
    if (hasOperation) {
      this.currentMetaColumn = HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION;
    } else {
      this.currentMetaColumn = HoodieRecord.HOODIE_META_COLUMNS;
    }
  }

  @Override
  public int numFields() {
    return currentMetaColumn.size() + row.numFields();
  }

  @Override
  public void setNullAt(int i) {
    if (i < currentMetaColumn.size()) {
      super.setNullAt(i);
    } else {
      row.setNullAt(i - currentMetaColumn.size());
    }
  }

  @Override
  public void update(int i, Object value) {
    if (i < currentMetaColumn.size()) {
      super.update(i, value);
    } else {
      row.update(i - currentMetaColumn.size(), value);
    }
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (ordinal < currentMetaColumn.size()) {
      return super.isNullAt(ordinal);
    }
    return row.isNullAt(ordinal - currentMetaColumn.size());
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return row.getBoolean(ordinal - currentMetaColumn.size());
  }

  @Override
  public byte getByte(int ordinal) {
    return row.getByte(ordinal - currentMetaColumn.size());
  }

  @Override
  public short getShort(int ordinal) {
    return row.getShort(ordinal - currentMetaColumn.size());
  }

  @Override
  public int getInt(int ordinal) {
    return row.getInt(ordinal - currentMetaColumn.size());
  }

  @Override
  public long getLong(int ordinal) {
    return row.getLong(ordinal - currentMetaColumn.size());
  }

  @Override
  public float getFloat(int ordinal) {
    return row.getFloat(ordinal - currentMetaColumn.size());
  }

  @Override
  public double getDouble(int ordinal) {
    return row.getDouble(ordinal - currentMetaColumn.size());
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return row.getDecimal(ordinal - currentMetaColumn.size(), precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (ordinal < currentMetaColumn.size()) {
      return super.getUTF8String(ordinal);
    }
    return row.getUTF8String(ordinal - currentMetaColumn.size());
  }

  @Override
  public String getString(int ordinal) {
    if (ordinal < currentMetaColumn.size()) {
      return super.getString(ordinal);
    }
    return row.getString(ordinal - currentMetaColumn.size());
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return row.getBinary(ordinal - currentMetaColumn.size());
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return row.getInterval(ordinal - currentMetaColumn.size());
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    return row.getStruct(ordinal - currentMetaColumn.size(), numFields);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    return row.getArray(ordinal - currentMetaColumn.size());
  }

  @Override
  public MapData getMap(int ordinal) {
    return row.getMap(ordinal - currentMetaColumn.size());
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (ordinal < currentMetaColumn.size()) {
      return super.get(ordinal, dataType);
    }
    return row.get(ordinal - currentMetaColumn.size(), dataType);
  }
}

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

package org.apache.hudi.data;

import org.apache.hudi.common.data.HoodieDataV2;
import org.apache.hudi.common.function.SerializableFunction;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import java.util.Iterator;
import java.util.List;

public class HoodieDataFrame extends HoodieDataV2<Row, Row> {

  private final Dataset<Row> df;

  private HoodieDataFrame(Dataset<Row> df) {
    this.df = df;
  }

  public static HoodieDataFrame of(Dataset<Row> df) {
    return new HoodieDataFrame(df);
  }

  @Override
  public Dataset<Row> get() {
    return df;
  }

  @Override
  public boolean isEmpty() {
    return df.isEmpty();
  }

  @Override
  public HoodieDataV2<Row, Row> map(SerializableFunction<Row, Row> func) {
    return HoodieDataFrame.of(df.map((MapFunction<Row, Row>) func, RowEncoder.apply(df.schema())));
  }

  @Override
  public HoodieDataV2<Row, Row> flatMap(SerializableFunction<Row, Iterator<Row>> func) {
    return null;
  }

  @Override
  public List<Row> collectAsList() {
    return df.collectAsList();
  }
}

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

package org.apache.hudi.hadoop.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetRecordReader;

import java.io.IOException;

public class HudiAvroParquetReader extends RecordReader<Void, ArrayWritable> {

  private final ParquetRecordReader<GenericData.Record> parquetRecordReader;

  public HudiAvroParquetReader(FilterCompat.Filter filter) {
    parquetRecordReader = new ParquetRecordReader<>(new AvroReadSupport<>(), filter);
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    parquetRecordReader.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return parquetRecordReader.nextKeyValue();
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return parquetRecordReader.getCurrentKey();
  }

  @Override
  public ArrayWritable getCurrentValue() throws IOException, InterruptedException {
    GenericRecord record = parquetRecordReader.getCurrentValue();
    return (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(record, record.getSchema());
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return parquetRecordReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    parquetRecordReader.close();
  }
}

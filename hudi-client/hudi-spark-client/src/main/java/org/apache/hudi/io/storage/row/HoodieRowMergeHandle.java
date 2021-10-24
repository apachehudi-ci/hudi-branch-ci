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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieMemoryConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import scala.Tuple2;

import static org.apache.hudi.client.utils.SparkDataFrameWriterUtils.fromMap;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_SEQNO_POS;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_POS;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_POS;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_POS;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_POS;
import static org.apache.hudi.config.HoodieIndexConfig.BLOOM_FILTER_FPP_VALUE;
import static org.apache.hudi.config.HoodieIndexConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE;
import static org.apache.hudi.config.HoodieIndexConfig.BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES;
import static org.apache.hudi.config.HoodieStorageConfig.PARQUET_BLOCK_SIZE;
import static org.apache.hudi.config.HoodieStorageConfig.PARQUET_PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_WRITER_VERSION;

public class HoodieRowMergeHandle implements Serializable, AutoCloseable {

  private final StructType schema;
  private final int partitionId;
  private final long taskId;
  private final long taskEpochId;
  private final String writeToken;
  private final String basePath;
  private final String instantTime;
  private final HoodieRowParquetWriteSupport writeSupport;
  private final Configuration hadoopConf;
  private int numFilesWritten;
  private int sequence;
  private Map<String, Map<Tuple2<String, String>, InternalRow>> existingFileAndUpdateRecords;
  private Tuple2<String, List<InternalRow>> newFileAndInsertRecords;
  private final HoodieInternalWriteStatus writeStatus;
  private final HoodieTimer timer;

  public HoodieRowMergeHandle(StructType schema, int partitionId, long taskId, long taskEpochId, String basePath,
      String instantTime, Map<String, String> options) {
    this.schema = schema;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.writeToken = FSUtils.makeWriteToken(partitionId, (int) taskId, taskEpochId);
    this.basePath = basePath;
    this.instantTime = instantTime;
    this.hadoopConf = fromMap(options);
    this.hadoopConf.set("org.apache.spark.sql.parquet.row.requested_schema", schema.json());
    this.hadoopConf.set("spark.sql.parquet.binaryAsString", "true");
    this.hadoopConf.set("spark.sql.parquet.int96AsTimestamp", "true");
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        Integer.parseInt(BLOOM_FILTER_NUM_ENTRIES_VALUE.defaultValue()),
        Double.parseDouble(BLOOM_FILTER_FPP_VALUE.defaultValue()),
        Integer.parseInt(BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES.defaultValue()),
        BloomFilterTypeCode.DYNAMIC_V0.name());
    this.writeSupport = new HoodieRowParquetWriteSupport(hadoopConf, schema, filter);
    this.existingFileAndUpdateRecords = new HashMap<>();
    this.writeStatus = new HoodieInternalWriteStatus(true, Double.parseDouble(options.get(HoodieMemoryConfig.WRITESTATUS_FAILURE_FRACTION.key())));
    // TODO(rxu) set more properties
    this.writeStatus.setStat(new HoodieWriteStat());
    this.timer = new HoodieTimer().startTimer();
    // TODO(rxu) write partition metadata
  }

  public void handle(Row row) {
    InternalRow record = GenericInternalRow.fromSeq(row.toSeq());
    String partitionPath = record.getUTF8String(PARTITION_PATH_POS).toString();
    if (isUpdate(record)) {
      String existingFileName = record.getUTF8String(FILENAME_POS).toString();
      if (!existingFileAndUpdateRecords.containsKey(existingFileName)) {
        existingFileAndUpdateRecords.put(existingFileName, new HashMap<>());
      }
      String key = record.getUTF8String(RECORD_KEY_POS).toString();
      String seqId = HoodieRecord.generateSequenceId(instantTime, partitionId, sequence++);
      String fileGroupId = fileGroupId(existingFileName);
      String newFileName = FSUtils.makeDataFileName(instantTime, writeToken,
          fileGroupId + "-" + numFilesWritten++, ".parquet");
      InternalRow recordWithMetaCols = updateMetaCols(record, instantTime, seqId, key, partitionPath, newFileName);
      existingFileAndUpdateRecords.get(existingFileName).put(new Tuple2<>(partitionPath, key), recordWithMetaCols);
    } else {
      if (newFileAndInsertRecords == null) {
        String newFileName = getFileName(UUID.randomUUID().toString());
        newFileAndInsertRecords = Tuple2.apply(newFileName, new ArrayList<>());
      }
      String key = record.getUTF8String(RECORD_KEY_POS).toString();
      String seqId = HoodieRecord.generateSequenceId(instantTime, partitionId, sequence++);
      InternalRow recordWithMetaCols = updateMetaCols(record, instantTime, seqId, key, partitionPath,
          newFileAndInsertRecords._1);
      newFileAndInsertRecords._2.add(recordWithMetaCols);
    }
  }

  public HoodieInternalWriteStatus flush() throws IOException {
    try {
      flushRecordsToFiles();
    } catch (Throwable t) {
      writeStatus.setGlobalError(t);
      throw t;
    }
    HoodieWriteStat stat = writeStatus.getStat();
    /*
    // TODO(rxu) add more stats
    stat.setPartitionPath(partitionPath);
    stat.setNumWrites(writeStatus.getTotalRecords());
    stat.setNumDeletes(0);
    stat.setNumInserts(writeStatus.getTotalRecords());
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(fileId);
    stat.setPath(new Path(writeConfig.getBasePath()), path);
    long fileSizeInBytes = FSUtils.getFileSize(table.getMetaClient().getFs(), path);
    stat.setTotalWriteBytes(fileSizeInBytes);
    stat.setFileSizeInBytes(fileSizeInBytes);
    stat.setTotalWriteErrors(writeStatus.getFailedRowsSize());
    */
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(timer.endTimer());
    stat.setRuntimeStats(runtimeStats);
    return writeStatus;
  }

  private void flushRecordsToFiles() throws IOException {
    String p = null;
    // write new files for inserts
    if (newFileAndInsertRecords != null) {
      String partitionPath = newFileAndInsertRecords._2.get(0).getUTF8String(3).toString();
      String newFileName = newFileAndInsertRecords._1;
      try (ParquetWriter<InternalRow> writer = newParquetWriter(partitionPath, newFileName)) {
        for (InternalRow r : newFileAndInsertRecords._2) {
          writeRow(writer, r, writeStatus);
        }
      }
      p = partitionPath;
    }
    // rewrite files for updates
    for (String existingFileName : existingFileAndUpdateRecords.keySet()) {
      List<InternalRow> updatingRecords = new ArrayList<>(
          existingFileAndUpdateRecords.get(existingFileName).values());
      String partitionPath = updatingRecords.get(0).getUTF8String(3).toString();
      String newFileName = updatingRecords.get(0).getUTF8String(4).toString();
      assert p == null || p.equals(partitionPath);
      Map<Tuple2<String, String>, InternalRow> existingRecords = readExistingFile(partitionPath,
          existingFileName);
      for (Tuple2<String, String> partitionKey : existingFileAndUpdateRecords.get(existingFileName).keySet()) {
        existingRecords.remove(partitionKey);
      }

      try (ParquetWriter<InternalRow> w = newParquetWriter(partitionPath, newFileName)) {
        for (InternalRow r : updatingRecords) {
          writeRow(w, r, writeStatus);
        }
        for (InternalRow r : existingRecords.values()) {
          writeRow(w, updateMetaCols(r, instantTime, HoodieRecord
              .generateSequenceId(instantTime, partitionId, sequence++), newFileName), writeStatus);
        }
      }
    }
  }

  public void close() throws IOException {
  }

  private ParquetWriter<InternalRow> newParquetWriter(String partitionPath, String fileName) throws IOException {
    Path file = new Path(new Path(basePath, partitionPath), fileName);
    return new ParquetWriter(file, writeSupport, CompressionCodecName.SNAPPY,
        Integer.parseInt(PARQUET_BLOCK_SIZE.defaultValue()), Integer.parseInt(PARQUET_PAGE_SIZE.defaultValue()),
        Integer.parseInt(PARQUET_PAGE_SIZE.defaultValue()), false, false, DEFAULT_WRITER_VERSION,
        FSUtils.registerFileSystem(file, this.writeSupport.getHadoopConf()));
  }

  private String getFileName(String fileId) {
    return FSUtils.makeDataFileName(instantTime, writeToken,
        fileId + "-" + numFilesWritten++, ".parquet");
  }

  private static void writeRow(ParquetWriter<InternalRow> writer, InternalRow r, HoodieInternalWriteStatus writeStatus) {
    String k = r.getUTF8String(RECORD_KEY_POS).toString();
    try {
      writer.write(r);
      writeStatus.markSuccess(k);
    } catch (Throwable t) {
      writeStatus.markFailure(k, t);
    }
  }

  private static boolean isUpdate(InternalRow record) {
    return !record.isNullAt(FILENAME_POS);
  }

  private static String fileGroupId(String fileName) {
    String fileGroupIdAndNumWrites = fileName.substring(0, fileName.indexOf('_'));
    return fileGroupIdAndNumWrites.substring(0, fileGroupIdAndNumWrites.lastIndexOf('-'));
  }

  private InternalRow updateMetaCols(InternalRow record, String commitTime, String seqId, String fileName) {
    InternalRow r = GenericInternalRow.fromSeq(record.toSeq(schema));
    r.update(COMMIT_TIME_POS, UTF8String.fromString(commitTime));
    r.update(COMMIT_SEQNO_POS, UTF8String.fromString(seqId));
    r.update(FILENAME_POS, UTF8String.fromString(fileName));
    return r;
  }

  private InternalRow updateMetaCols(InternalRow record, String commitTime, String seqId, String key,
      String partitionPath, String fileName) {
    InternalRow r = GenericInternalRow.fromSeq(record.toSeq(schema));
    r.update(COMMIT_TIME_POS, UTF8String.fromString(commitTime));
    r.update(COMMIT_SEQNO_POS, UTF8String.fromString(seqId));
    r.update(RECORD_KEY_POS, UTF8String.fromString(key));
    r.update(PARTITION_PATH_POS, UTF8String.fromString(partitionPath));
    r.update(FILENAME_POS, UTF8String.fromString(fileName));
    return r;
  }

  private Map<Tuple2<String, String>, InternalRow> readExistingFile(String partitionPath, String existingFileName)
      throws IOException {
    Path existingFile = new Path(new Path(basePath, partitionPath), existingFileName);
    ParquetReadSupport readSupport = new ParquetReadSupport();
    try (ParquetFileReader parquetFileReader = ParquetFileReader
        .open(HadoopInputFile.fromPath(existingFile, hadoopConf))) {
      MessageType schema = parquetFileReader.getFooter().getFileMetaData().getSchema();
      InitContext ctx = new InitContext(hadoopConf, null, schema);
      ReadSupport.ReadContext readCtx = readSupport.init(ctx);
      readSupport.prepareForRead(hadoopConf, null, schema, readCtx);
    }
    Map<Tuple2<String, String>, InternalRow> partitionKeyToExistingRecords = new HashMap<>();
    try (ParquetReader<InternalRow> reader = new ParquetReader(hadoopConf, existingFile, readSupport)) {
      while (true) {
        InternalRow r = reader.read();
        if (r == null) {
          break;
        }
        String key = r.getUTF8String(RECORD_KEY_POS).toString();
        partitionKeyToExistingRecords.put(new Tuple2<>(partitionPath, key), r);
      }
    }
    return partitionKeyToExistingRecords;
  }
}

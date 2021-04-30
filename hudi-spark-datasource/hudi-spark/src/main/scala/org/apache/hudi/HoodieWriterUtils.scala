/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig.{ENABLE, VALIDATE_ENABLE}
import org.apache.hudi.common.config.{HoodieConfig, TypedProperties}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory

import java.util.Properties
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters.{mapAsScalaMapConverter, _}

/**
 * WriterUtils to assist in write path in Datasource and tests.
 */
object HoodieWriterUtils {

  def javaParametersWithWriteDefaults(parameters: java.util.Map[String, String]): java.util.Map[String, String] = {
    mapAsJavaMap(parametersWithWriteDefaults(parameters.asScala.toMap))
  }

  /**
    * Add default options for unspecified write options keys.
    *
    * @param parameters
    * @return
    */
  def parametersWithWriteDefaults(parameters: Map[String, String]): Map[String, String] = {
    val props = new Properties()
    props.putAll(parameters)
    val hoodieConfig: HoodieConfig = new HoodieConfig(props)
    hoodieConfig.setDefaultValue(OPERATION)
    hoodieConfig.setDefaultValue(TABLE_TYPE)
    hoodieConfig.setDefaultValue(PRECOMBINE_FIELD)
    hoodieConfig.setDefaultValue(PAYLOAD_CLASS_NAME)
    hoodieConfig.setDefaultValue(RECORDKEY_FIELD)
    hoodieConfig.setDefaultValue(PARTITIONPATH_FIELD)
    hoodieConfig.setDefaultValue(KEYGENERATOR_CLASS_NAME)
    hoodieConfig.setDefaultValue(ENABLE)
    hoodieConfig.setDefaultValue(VALIDATE_ENABLE)
    hoodieConfig.setDefaultValue(COMMIT_METADATA_KEYPREFIX)
    hoodieConfig.setDefaultValue(INSERT_DROP_DUPS)
    hoodieConfig.setDefaultValue(STREAMING_RETRY_CNT)
    hoodieConfig.setDefaultValue(STREAMING_RETRY_INTERVAL_MS)
    hoodieConfig.setDefaultValue(STREAMING_IGNORE_FAILED_BATCH)
    hoodieConfig.setDefaultValue(META_SYNC_CLIENT_TOOL_CLASS_NAME)
    hoodieConfig.setDefaultValue(HIVE_SYNC_ENABLED)
    hoodieConfig.setDefaultValue(META_SYNC_ENABLED)
    hoodieConfig.setDefaultValue(HIVE_DATABASE)
    hoodieConfig.setDefaultValue(HIVE_TABLE)
    hoodieConfig.setDefaultValue(HIVE_BASE_FILE_FORMAT)
    hoodieConfig.setDefaultValue(HIVE_USER)
    hoodieConfig.setDefaultValue(HIVE_PASS)
    hoodieConfig.setDefaultValue(HIVE_URL)
    hoodieConfig.setDefaultValue(HIVE_PARTITION_FIELDS)
    hoodieConfig.setDefaultValue(HIVE_PARTITION_EXTRACTOR_CLASS)
    hoodieConfig.setDefaultValue(HIVE_STYLE_PARTITIONING)
    hoodieConfig.setDefaultValue(HIVE_USE_JDBC)
    hoodieConfig.setDefaultValue(HIVE_CREATE_MANAGED_TABLE)
    hoodieConfig.setDefaultValue(HIVE_SYNC_AS_DATA_SOURCE_TABLE)
    hoodieConfig.setDefaultValue(ASYNC_COMPACT_ENABLE)
    hoodieConfig.setDefaultValue(INLINE_CLUSTERING_ENABLE)
    hoodieConfig.setDefaultValue(ASYNC_CLUSTERING_ENABLE)
    hoodieConfig.setDefaultValue(ENABLE_ROW_WRITER)
    hoodieConfig.setDefaultValue(RECONCILE_SCHEMA)
    hoodieConfig.setDefaultValue(DROP_PARTITION_COLUMNS)
    Map() ++ hoodieConfig.getProps.asScala ++ DataSourceOptionsHelper.translateConfigurations(parameters)
  }

  def toProperties(params: Map[String, String]): TypedProperties = {
    val props = new TypedProperties()
    params.foreach(kv => props.setProperty(kv._1, kv._2))
    props
  }

  /**
   * Get the partition columns to stored to hoodie.properties.
   * @param parameters
   * @return
   */
  def getPartitionColumns(parameters: Map[String, String]): String = {
    val props = new TypedProperties()
    props.putAll(parameters.asJava)
    val keyGen = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props)
    HoodieSparkUtils.getPartitionColumns(keyGen, props)
  }

  def convertMapToHoodieConfig(parameters: Map[String, String]): HoodieConfig = {
    val properties = new Properties()
    properties.putAll(mapAsJavaMap(parameters))
    new HoodieConfig(properties)
  }
}

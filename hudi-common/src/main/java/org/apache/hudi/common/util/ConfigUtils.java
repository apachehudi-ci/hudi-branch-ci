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

package org.apache.hudi.common.util;

import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;

import java.util.Properties;

public class ConfigUtils {

  /**
   * Get ordering field.
   */
  public static String getOrderingField(Properties properties) {
    String orderField = null;
    if (properties.containsKey(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY)) {
      orderField = properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
    } else if (properties.containsKey("hoodie.datasource.write.precombine.field")) {
      orderField = properties.getProperty("hoodie.datasource.write.precombine.field");
    } else if (properties.containsKey(HoodieTableConfig.PRECOMBINE_FIELD.key())) {
      orderField = properties.getProperty(HoodieTableConfig.PRECOMBINE_FIELD.key());
    }
    return orderField;
  }

  /**
   * Get merger without engin type.
   */
  public static HoodieRecordMerger generateRecordMerger(String basePath, EngineType engineType, Option<String> mergerClass) {
    if (mergerClass.isPresent()) {
      return HoodieRecordUtils.loadRecordMerger(mergerClass.get(), basePath);
    } else if (engineType == EngineType.SPARK) {
      return ReflectionUtils.loadClass("org.apache.hudi.HoodieSparkRecordMerger");
    } else {
      return ReflectionUtils.loadClass(HoodieAvroRecordMerger.class.getName());
    }
  }

  /**
   * Get merger without engin type.
   */
  public static HoodieRecordMerger generateRecordMerger(String basePath, Option<String> mergerClass) {
    if (mergerClass.isPresent()) {
      return HoodieRecordUtils.loadRecordMerger(mergerClass.get(), basePath);
    } else {
      return ReflectionUtils.loadClass(HoodieAvroRecordMerger.class.getName());
    }
  }

  /**
   * Get merger without engin type.
   */
  public static String getRecordMergerClass(EngineType engineType, boolean adaptionMergeClass, Option<String> mergerClass) {
    if (mergerClass.isPresent()) {
      return mergerClass.get();
    } else if (adaptionMergeClass && engineType == EngineType.SPARK) {
      return "org.apache.hudi.HoodieSparkRecordMerger";
    } else {
      return HoodieAvroRecordMerger.class.getName();
    }
  }
}

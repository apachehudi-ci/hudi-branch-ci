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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Hoodie Configs for Data layout optimize.
 */
public class HoodieOptimizeConfig extends HoodieConfig {
  // Any Data layout optimize params can be saved with this prefix
  public static final String DATA_LAYOUT_OPTIMIZE_PARAM_PREFIX = "hoodie.data.layout.optimize.";
  public static final ConfigProperty<String> DATA_LAYOUT_STRATEGY = ConfigProperty
      .key(DATA_LAYOUT_OPTIMIZE_PARAM_PREFIX + "strategy")
      .defaultValue("z-order")
      .sinceVersion("0.10.0")
      .withDocumentation("config to provide a way to optimize data layout for table, current only support z-order and hilbert");

  public static final ConfigProperty<String> DATA_LAYOUT_BUILD_CURVE_STRATEGY = ConfigProperty
      .key(DATA_LAYOUT_OPTIMIZE_PARAM_PREFIX + "build.curve.optimize.strategy")
      .defaultValue("directly")
      .sinceVersion("0.10.0")
      .withDocumentation("Config to provide whether use directly/sample method to build curve optimize for data layout,"
          + "build curve_optimize by directly method is faster than by sample method, however sample method produce a better data layout."
          + "now support two strategies: directly,sample");

  public static final ConfigProperty<String> DATA_LAYOUT_CURVE_OPTIMIZE_SAMPLE_NUMBER = ConfigProperty
      .key(DATA_LAYOUT_OPTIMIZE_PARAM_PREFIX + "curve.optimize.sample.number")
      .defaultValue("200000")
      .sinceVersion("0.10.0")
      .withDocumentation("when set" + DATA_LAYOUT_BUILD_CURVE_STRATEGY.key() + " to sample method, sample number need to be set for it."
          + " larger number means better layout result, but more memory consumer");

  public static final ConfigProperty<String> DATA_LAYOUT_CURVE_OPTIMIZE_SORT_COLUMNS = ConfigProperty
      .key(DATA_LAYOUT_OPTIMIZE_PARAM_PREFIX + "curve.optimize.sort.columns")
      .defaultValue("")
      .sinceVersion("0.10.0")
      .withDocumentation("sort columns for build curve optimize. default value is empty string which means no sort."
          + " more sort columns you specify, the worse data layout result. No more than 4 are recommended");

  public static final ConfigProperty<Boolean> DATA_LAYOUT_DATA_SKIPPING_ENABLE = ConfigProperty
      .key(DATA_LAYOUT_OPTIMIZE_PARAM_PREFIX + "data.skipping.enable")
      .defaultValue(true)
      .sinceVersion("0.10.0")
      .withDocumentation("enable dataSkipping for hudi, when optimize finished, statistics will be collected which used for dataSkipping");

  public static final ConfigProperty<String> DATA_LAYOUT_DATA_STATISTICS_SAVE_MODE = ConfigProperty
      .key(DATA_LAYOUT_OPTIMIZE_PARAM_PREFIX + "statistics.save.mode")
      .defaultValue("append")
      .sinceVersion("0.10.0")
      .withDocumentation("how to save statistics info every time, when do optimize. now support two modes: append, overwrite");

  private HoodieOptimizeConfig() {
    super();
  }

  public static HoodieOptimizeConfig.Builder newBuilder() {
    return new HoodieOptimizeConfig.Builder();
  }

  public static class Builder {

    private final HoodieOptimizeConfig optimizeConfig = new HoodieOptimizeConfig();

    public HoodieOptimizeConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.optimizeConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder withOptimizeDataLayOutStrategy(String strategy) {
      optimizeConfig.setValue(DATA_LAYOUT_STRATEGY, strategy);
      return this;
    }

    public Builder withOptimizeBuildCurveOptimizeMethod(String method) {
      optimizeConfig.setValue(DATA_LAYOUT_BUILD_CURVE_STRATEGY, method);
      return this;
    }

    public Builder withCurveOptimizeSampleNumber(int sampleNumber) {
      optimizeConfig.setValue(DATA_LAYOUT_CURVE_OPTIMIZE_SAMPLE_NUMBER, String.valueOf(sampleNumber));
      return this;
    }

    public Builder withCurveOptimizeSortColumns(String sortColumns) {
      optimizeConfig.setValue(DATA_LAYOUT_CURVE_OPTIMIZE_SORT_COLUMNS, sortColumns);
      return this;
    }

    public Builder withOptimizeEnableDataSkipping(boolean dataSkipping) {
      optimizeConfig.setValue(DATA_LAYOUT_DATA_SKIPPING_ENABLE, String.valueOf(dataSkipping));
      return this;
    }

    public Builder withOptimizeStatisticsSaveMode(String saveMode) {
      optimizeConfig.setValue(DATA_LAYOUT_DATA_STATISTICS_SAVE_MODE, saveMode);
      return this;
    }

    public Builder fromProperties(Properties props) {
      this.optimizeConfig.getProps().putAll(props);
      return this;
    }

    public HoodieOptimizeConfig build() {
      optimizeConfig.setDefaults(HoodieOptimizeConfig.class.getName());
      return optimizeConfig;
    }
  }
}

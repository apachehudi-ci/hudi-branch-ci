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

package org.apache.spark.sql.operation

import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.{HoodieOptimizeConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.{DataFrame, OptimizeTableByCurve, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, ExpressionSet, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class OptimizeHudiTableByCurve(
    target: LogicalPlan,
    condition: Option[Expression],
    orderFields: Seq[Expression],
    outputFileNum: Option[Int],
    options: Option[Map[String, String]],
    curveName: String = "z-order") extends OptimizeTableByCurve(target, condition, orderFields, outputFileNum, options, curveName) {

  var sourceHudiTablePath: String = ""
  var sinkHudiTablePath: String = ""

  override def getOutputFileNum: String = {
    options.getOrElse(Map.empty).getOrElse(HoodieWriteConfig.BULKINSERT_PARALLELISM, super.getOutputFileNum.toString)
  }

  override def splitPartitionAndDataPredicates(
      target: LogicalPlan,
      condition: Expression,
      sparkSession: SparkSession): (Seq[Expression], Seq[Expression]) = {

    val (path, partitions) = TableOperationsUtils.extractPathAndPartitionFromRelation(target, sparkSession)
    sourceHudiTablePath = path

    if (condition == Literal.TrueLiteral) {
      (Seq(condition), Seq())
    } else {

      if (sourceHudiTablePath.nonEmpty) {
        val resolvePartition = partitions.map(p => target.resolve(Seq(p), sparkSession.sessionState.analyzer.resolver).get)
        val partitionSet = AttributeSet(resolvePartition)
        val filters = ExpressionSet(splitConjunctivePredicates(condition).filter(_.deterministic))
        val partitionKeyFilters = ExpressionSet(filters
          .filter(_.references.subsetOf(partitionSet)))
        val dataFilters =
          filters.filter(_.references.intersect(partitionSet).isEmpty)
        (partitionKeyFilters.toSeq, dataFilters.toSeq)
      } else {
        (Seq(condition), Seq())
      }
    }
  }

  override def doOptimize(df: DataFrame, options: Option[Map[String, String]], z_orderFields: Seq[String]): Unit = {
    if (sinkHudiTablePath.isEmpty) {
      sinkHudiTablePath = sourceHudiTablePath
    }
    if (sinkHudiTablePath.isEmpty) {
      new HoodieException("cannot save z data to empty hudi table or non Hudi table")
    }

    val metaClient = HoodieTableMetaClient.builder().
      setBasePath(sinkHudiTablePath).setConf(df.sparkSession.sparkContext.hadoopConfiguration).build()
    df.write.format("hudi").
      options(options.get).
      option(HoodieOptimizeConfig.DATA_LAYOUT_CURVE_OPTIMIZE_SORT_COLUMNS.key, z_orderFields.mkString(",")).
      option(HoodieWriteConfig.BULKINSERT_PARALLELISM, getOutputFileNum).
      option(HoodieWriteConfig.TBL_NAME.key(), metaClient.getTableConfig.getTableName).mode(SaveMode.Append).save(sinkHudiTablePath)
  }
}

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

package org.apache.spark.sql

import org.apache.hudi.config.HoodieOptimizeConfig
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand

abstract class OptimizeTableByCurve(
    target: LogicalPlan,
    condition: Option[Expression],
    orderFields: Seq[Expression],
    outputFileNum: Option[Int],
    options: Option[Map[String, String]],
    curveName: String = "z-order") extends RunnableCommand with Logging with PredicateHelper {

  val z_orderFields = orderFields.map(Zoptimize.getTargetColNameParts(_).mkString("."))

  val optimizeCondition = condition.getOrElse(Literal.TrueLiteral)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val optimizeDF = curveName match {
      case "z-order" =>
        buildZData(sparkSession)
      // to do support hilbert curve
      case other =>
        throw new SparkException(s"only support z-order optimize!!!! but find: ${other} optimize")
    }
    doOptimize(optimizeDF, options, z_orderFields)
    Seq.empty[Row]
  }

  def buildZData(sparkSession: SparkSession): DataFrame = {

    val (partitionPredicates, dataPredicates) = splitPartitionAndDataPredicates(target, optimizeCondition, sparkSession)
    if (dataPredicates.nonEmpty) {
      throw new SparkException(s"only support partitionPredicates for optimizer Curve," +
        s" but find other Predicates: ${dataPredicates.map(_.toString()).mkString("::")} ")
    }
    val df = Dataset.ofRows(sparkSession, Project(target.output, Filter(partitionPredicates.reduce(And), target)))
    if (options.isDefined && options.get.getOrElse(HoodieOptimizeConfig.DATA_LAYOUT_BUILD_CURVE_STRATEGY.key,
      HoodieOptimizeConfig.DATA_LAYOUT_BUILD_CURVE_STRATEGY.defaultValue).equals("sample")) {
      Zoptimize.createZIndexedDataFrameBySample(df, z_orderFields , getOutputFileNum.toInt)
    } else {
      Zoptimize.createZIndexedDataFrameByMapValue(df, z_orderFields, getOutputFileNum.toInt)
    }
  }

  def getOutputFileNum: String = {
    outputFileNum.getOrElse(200).toString
  }

  def splitPartitionAndDataPredicates(
      target: LogicalPlan,
      condition: Expression,
      sparkSession: SparkSession): (Seq[Expression], Seq[Expression]) = {
    (Seq(Literal.TrueLiteral), Seq())
  }

  def doOptimize(df: DataFrame, options: Option[Map[String, String]] = None, z_orderFields: Seq[String]): Unit = {
    // no-op
  }
}

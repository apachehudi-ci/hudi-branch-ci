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

import org.apache.hadoop.fs.Path
import org.apache.hudi.{DataSourceWriteOptions, HoodieBootstrapRelation, HoodieFileIndex, MergeOnReadSnapshotRelation}
import org.apache.hudi.exception.HoodieException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExtractValue, GetStructField}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation

class HudiTableOperations(df: DataFrame) extends Logging with Serializable {

  def optimizeByZOrder(
      zorderBy: Seq[String],
      options: Map[String, String],
      condition: Column = null,
      outputFileNum: Int = 200): Unit = {
    checkWriteOptions(if (options == null) Map.empty else options)
    val conditionOpt = if (condition == null) None else Some(condition)
    executeZorderOptimize(conditionOpt, zorderBy, Some(outputFileNum), Some(options))
  }

  private def executeZorderOptimize(
      condition: Option[Column],
      zorderBy: Seq[String],
      outputFileNum: Option[Int],
      options: Option[Map[String, String]]): Unit = {
    val zCols = zorderBy.map(UnresolvedAttribute.quotedString)

    val optimizeCurve = OptimizeCurve(df.queryExecution.analyzed, condition.map(_.expr), zCols, outputFileNum, "z-order")

    val r = TableOperationsUtils.resolveOptimizeCurveReferences(optimizeCurve,
      df.sparkSession.sessionState.conf)(TableOperationsUtils.tryResolveReferences(df.sparkSession) _)

    OptimizeHudiTableByCurve(
      EliminateSubqueryAliases(r.target), r.condition, r.orderFields, r.outputFileNum, options, r.curveName).run(df.sparkSession)
  }

  private def checkWriteOptions(parameters: Map[String, String]): Unit = {
    def checkNeededOption(option: String): Unit = {
      parameters.getOrElse(option, throw new HoodieException(s"cannot find ${option}, pls set it when you create hudi table"))
    }
    checkNeededOption(DataSourceWriteOptions.PRECOMBINE_FIELD.key())
    checkNeededOption(DataSourceWriteOptions.RECORDKEY_FIELD.key())
    checkNeededOption(DataSourceWriteOptions.PARTITIONPATH_FIELD.key())
  }
}

case class OptimizeCurve(
    target: LogicalPlan,
    condition: Option[Expression],
    orderFields: Seq[Expression],
    outputFileNum: Option[Int],
    curveName: String = "z-order") extends Command {

  override def children: Seq[LogicalPlan] = Seq(target)

  override def output: Seq[Attribute] = Seq.empty

}

object TableOperationsUtils {

  /** LogicalPlan to help resolve the given expression */
  case class FakeLogicalPlan(expr: Expression, children: Seq[LogicalPlan])
    extends LogicalPlan {
    override def output: Seq[Attribute] = Nil
  }

  def tryResolveReferences(spark: SparkSession)(expr: Expression, plan: LogicalPlan): Expression = {
    val newPlan = FakeLogicalPlan(expr, plan.children)
    spark.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolveExpr, _) =>
        // scalastyle:off
        return resolveExpr
      // scalastyle:on
      case _ =>
        throw new AnalysisException(s"cannot resolve expression ${expr}")
    }
  }

  /**
    * Extracts name from a resolved expression referring to a nested or non-nested column
    */
  def getTargetColNameParts(resolvedTargetCol: Expression): Seq[String] = {

    resolvedTargetCol match {
      case attr: Attribute => Seq(attr.name)

      case Alias(c, _) => getTargetColNameParts(c)

      case GetStructField(c, _, Some(name)) => getTargetColNameParts(c) :+ name

      case ex: ExtractValue =>
        throw new AnalysisException(s"convert reference to name failed, Updating nested fields is only supported for StructType: ${ex}.")

      case other =>
        throw new AnalysisException(s"convert reference to name failed,  Found unsupported expression ${other}")
    }
  }

  def resolveOptimizeCurveReferences(
      optimizeCurve: OptimizeCurve,
      conf: SQLConf)(resolveExpr: (Expression, LogicalPlan) => Expression): OptimizeCurve = {
    val OptimizeCurve(target, condition, orderFields, _, _) = optimizeCurve

    val fakeTargetPlan = Project(target.output, target)

    def resolveOrFail(expr: Expression, plan: LogicalPlan): Expression = {
      val resolvedExpr = resolveExpr(expr, plan)
      resolvedExpr.flatMap(_.references).filter(!_.resolved).foreach { a =>
        val cols = "columns " + plan.children.flatMap(_.output).map(_.sql).mkString(", ")
        throw new AnalysisException(s"cannot resolve ${a.sql} for given $cols")
      }
      resolvedExpr
    }

    // resolve condition
    val resolvedCondition = condition.map(resolveOrFail(_, fakeTargetPlan))

    // resolve orderFields, only for check
    val resolvedOrderFields = orderFields.map(resolveOrFail(_, fakeTargetPlan))

    optimizeCurve.copy(condition = resolvedCondition, orderFields = resolvedOrderFields)
  }

  def extractPathAndPartitionFromRelation(target: LogicalPlan, sparkSession: SparkSession): (String, Seq[String]) = {
    target.collect {
      case h: HiveTableRelation =>
        val table = h.asInstanceOf[HiveTableRelation].tableMeta
        if ((table.storage.inputFormat.getOrElse("").contains("Hoodie"))) {
          (getTablePath(sparkSession, table), table.partitionColumnNames)
        } else {
          ("", Seq.empty)
        }

      case l @ LogicalRelation(r: BaseRelation, _, _, _) =>
        r match {
          case h @ HadoopFsRelation(index: HoodieFileIndex, _, _, _, _, _) =>
            val tableConfig = index.metaClient.getTableConfig
            val partitionColumns = tableConfig.getPartitionFields
            (index.metaClient.getBasePath, if (partitionColumns.isPresent) partitionColumns.get().toSeq else Seq.empty)
          case m : MergeOnReadSnapshotRelation =>
            val tableConfig = m.metaClient.getTableConfig
            val partitionColumns = tableConfig.getPartitionFields
            (m.metaClient.getBasePath, if (partitionColumns.isPresent) partitionColumns.get().toSeq else Seq.empty)
          case b : HoodieBootstrapRelation =>
            val tableConfig = b.metaClient.getTableConfig
            val partitionColumns = tableConfig.getPartitionFields
            (b.metaClient.getBasePath, if (partitionColumns.isPresent) partitionColumns.get().toSeq else Seq.empty)
          case _ => ("", Seq.empty)
        }
    }.headOption.getOrElse(("", Seq.empty))
  }

  def getTablePath(spark: SparkSession, table: CatalogTable): String = {
    val url = if (table.tableType == CatalogTableType.MANAGED) {
      Some(spark.sessionState.catalog.defaultTablePath(table.identifier))
    } else {
      table.storage.locationUri
    }
    val fs = new Path(url.get).getFileSystem(spark.sparkContext.hadoopConfiguration)
    val rawPath = fs.makeQualified(new Path(url.get)).toUri.toString
    // remove placeHolder
    if (rawPath.endsWith("-PLACEHOLDER")) {
      rawPath.substring(0, rawPath.length() - 16)
    } else {
      rawPath
    }
  }
}
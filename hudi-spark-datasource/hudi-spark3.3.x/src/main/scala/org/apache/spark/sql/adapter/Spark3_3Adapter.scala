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

package org.apache.spark.sql.adapter

import org.apache.avro.Schema
import org.apache.hudi.Spark33HoodieFileScanRDD
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, Spark33HoodieParquetFileFormat}
import org.apache.spark.sql.parser.HoodieSpark3_3ExtendedSqlParser
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, HoodieSpark3_3CatalystExpressionUtils, SparkSession}

/**
 * Implementation of [[SparkAdapter]] for Spark 3.3.x branch
 */
class Spark3_3Adapter extends BaseSpark3Adapter {

  override def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer =
    new HoodieSpark3_2AvroSerializer(rootCatalystType, rootAvroType, nullable)

  override def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer =
    new HoodieSpark3_2AvroDeserializer(rootAvroType, rootCatalystType)

  override def createCatalystExpressionUtils(): HoodieCatalystExpressionUtils = HoodieSpark3_3CatalystExpressionUtils

  /**
   * if the logical plan is a TimeTravelRelation LogicalPlan.
   */
  override def isRelationTimeTravel(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[TimeTravelRelation]
  }

  /**
   * Get the member of the TimeTravelRelation LogicalPlan.
   */
  override def getRelationTimeTravel(plan: LogicalPlan): Option[(LogicalPlan, Option[Expression], Option[String])] = {
    plan match {
      case timeTravel: TimeTravelRelation =>
        Some((timeTravel.table, timeTravel.timestamp, timeTravel.version))
      case _ =>
        None
    }
  }

  override def createExtendedSparkParser: Option[(SparkSession, ParserInterface) => ParserInterface] = {
    Some(
      (spark: SparkSession, delegate: ParserInterface) => new HoodieSpark3_3ExtendedSqlParser(spark, delegate)
    )
  }

  override def createHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat] = {
    Some(new Spark33HoodieParquetFileFormat(appendPartitionValues))
  }

  override def createHoodieFileScanRDD(sparkSession: SparkSession,
                                       readFunction: PartitionedFile => Iterator[InternalRow],
                                       filePartitions: Seq[FilePartition],
                                       readDataSchema: StructType,
                                       metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD = {
    new Spark33HoodieFileScanRDD(sparkSession, readFunction, filePartitions, readDataSchema, metadataColumns)
  }


  override def resolveDeleteFromTable(dft: Command,
                                      resolveExpression: Expression => Expression): DeleteFromTable = {
    val deleteFromTableCommand = dft.asInstanceOf[DeleteFromTable]
    DeleteFromTable(deleteFromTableCommand.table, resolveExpression(deleteFromTableCommand.condition))
  }

  override def getQueryParserFromExtendedSqlParser(session: SparkSession, delegate: ParserInterface,
                                                   sqlText: String): LogicalPlan = {
    new HoodieSpark3_3ExtendedSqlParser(session, delegate).parseQuery(sqlText)
  }
}

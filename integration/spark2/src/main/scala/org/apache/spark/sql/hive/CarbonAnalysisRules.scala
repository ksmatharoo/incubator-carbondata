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

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.CarbonExpressions.{CarbonUnresolvedRelation, MatchCast}
import org.apache.spark.sql.catalyst.{CarbonTableIdentifierImplicit, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.command.mutation.CarbonProjectForDeleteCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.core.constants.CarbonCommonConstants

case class CarbonIUDAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private lazy val parser = sparkSession.sessionState.sqlParser

  private def processUpdateQuery(
      table: UnresolvedRelation,
      columns: List[String],
      selectStmt: String,
      alias: Option[String],
      filter: String): LogicalPlan = {
    var includedDestColumns = false
    var includedDestRelation = false
    var addedTupleId = false

    def prepareTargetReleation(relation: UnresolvedRelation): SubqueryAlias = {
      val tupleId = UnresolvedAlias(Alias(UnresolvedFunction("getTupleId",
        Seq.empty, isDistinct = false), "tupleId")())

      val projList = Seq(UnresolvedAlias(UnresolvedStar(alias.map(Seq(_)))), tupleId)

      CarbonReflectionUtils.getSubqueryAlias(
        sparkSession,
        alias,
        Project(projList, relation),
        Some(table.tableIdentifier))
    }

    // get the un-analyzed logical plan
    val targetTable = prepareTargetReleation(table)
    val selectPlan = parser.parsePlan(selectStmt) transform {
      case Project(projectList, child) if !includedDestColumns =>
        includedDestColumns = true
        if (projectList.size != columns.size) {
          CarbonException.analysisException(
            "The number of columns in source table and destination table columns mismatch")
        }
        val renamedProjectList = projectList.zip(columns).map { case (attr, col) =>
          attr match {
            case UnresolvedAlias(child22, _) =>
              UnresolvedAlias(Alias(child22, col + "-updatedColumn")())
            case UnresolvedAttribute(param) =>
              UnresolvedAlias(Alias(attr, col + "-updatedColumn")())
            case _ => attr
          }
        }
        val tableName: Option[Seq[String]] = alias match {
          case Some(a) => Some(alias.toSeq)
          case _ => Some(Seq(child.asInstanceOf[UnresolvedRelation].tableIdentifier.table.toString))
        }
        val list = Seq(
          UnresolvedAlias(UnresolvedStar(tableName))) ++ renamedProjectList
        Project(list, child)
      case Filter(cond, child) if !includedDestRelation =>
        includedDestRelation = true
        Filter(cond, Join(child, targetTable, Inner, None))
      case r@CarbonUnresolvedRelation(t) if !includedDestRelation && t != table.tableIdentifier =>
        includedDestRelation = true
        Join(r, targetTable, Inner, None)
    }
    val updatedSelectPlan: LogicalPlan = if (!includedDestRelation) {
      // special case to handle self join queries
      // Eg. update tableName  SET (column1) = (column1+1)
      selectPlan transform {
        case relation: UnresolvedRelation
          if table.tableIdentifier == relation.tableIdentifier && !addedTupleId =>
          addedTupleId = true
          targetTable
      }
    } else {
      selectPlan
    }
    val finalPlan = if (filter.length > 0) {
      var transformed: Boolean = false
      // Create a dummy projection to include filter conditions
      var newPlan: LogicalPlan = null
      if (table.tableIdentifier.database.isDefined) {
        newPlan = parser.parsePlan("select * from  " +
           table.tableIdentifier.database.getOrElse("") + "." +
           table.tableIdentifier.table + " " + alias.getOrElse("") + " " + filter)
      }
      else {
        newPlan = parser.parsePlan("select * from  " +
           table.tableIdentifier.table + " " + alias.getOrElse("") + " " + filter)
      }
      newPlan transform {
        case CarbonUnresolvedRelation(t)
          if !transformed && t == table.tableIdentifier =>
          transformed = true

          CarbonReflectionUtils.getSubqueryAlias(
            sparkSession,
            alias,
            updatedSelectPlan,
            Some(table.tableIdentifier))
      }
    } else {
      updatedSelectPlan
    }
    val tid = CarbonTableIdentifierImplicit.toTableIdentifier(Seq(table.tableIdentifier.toString()))
    val tidSeq = Seq(GetDB.getDatabaseName(tid.database, sparkSession))
    val destinationTable =
      CarbonReflectionUtils.getUnresolvedRelation(
        table.tableIdentifier,
        sparkSession.version,
        alias)

    ProjectForUpdate(destinationTable, columns, Seq(finalPlan))
  }


  def processDeleteRecordsQuery(selectStmt: String,
      alias: Option[String],
      table: UnresolvedRelation): LogicalPlan = {
    val tidSeq = Seq(GetDB.getDatabaseName(table.tableIdentifier.database, sparkSession),
      table.tableIdentifier.table)
    var addedTupleId = false
    val parsePlan = parser.parsePlan(selectStmt)

    val selectPlan = parsePlan transform {
      case relation: UnresolvedRelation
        if table.tableIdentifier == relation.tableIdentifier && !addedTupleId =>
        addedTupleId = true
        val tupleId = UnresolvedAlias(Alias(UnresolvedFunction("getTupleId",
          Seq.empty, isDistinct = false), "tupleId")())

        val projList = Seq(UnresolvedAlias(UnresolvedStar(alias.map(Seq(_)))), tupleId)
        // include tuple id in subquery
        Project(projList, relation)
    }
    CarbonProjectForDeleteCommand(
      selectPlan,
      tidSeq,
      System.currentTimeMillis().toString)
  }

  override def apply(logicalplan: LogicalPlan): LogicalPlan = {

    logicalplan transform {
      case UpdateTable(t, cols, sel, alias, where) => processUpdateQuery(t, cols, sel, alias, where)
      case DeleteRecords(statement, alias, table) =>
        processDeleteRecordsQuery(
          statement,
          alias,
          table)
    }
  }
}

/**
 * Insert into carbon table from other source
 */
case class CarbonPreInsertionCasts(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p@InsertIntoTable(relation: LogicalRelation, _, child, _, _)
        if relation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        castChildOutput(p, relation, child)
    }
  }

  def castChildOutput(p: InsertIntoTable,
      relation: LogicalRelation,
      child: LogicalPlan)
  : LogicalPlan = {
    val carbonDSRelation = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    if (carbonDSRelation.carbonRelation.output.size > CarbonCommonConstants
      .DEFAULT_MAX_NUMBER_OF_COLUMNS) {
      CarbonException.analysisException("Maximum number of columns supported:" +
                                        s"${CarbonCommonConstants.DEFAULT_MAX_NUMBER_OF_COLUMNS}")
    }
    val isAggregateTable = !carbonDSRelation.carbonRelation.carbonTable.getTableInfo
      .getParentRelationIdentifiers.isEmpty
    // transform logical plan if the load is for aggregate table.
    val childPlan = if (isAggregateTable) {
      transformAggregatePlan(child)
    } else {
      child
    }
    if (childPlan.output.size >= carbonDSRelation.carbonRelation.output.size) {
      val newChildOutput = childPlan.output.zipWithIndex.map { columnWithIndex =>
        columnWithIndex._1 match {
          case attr: Alias =>
            Alias(attr.child, s"col${ columnWithIndex._2 }")(attr.exprId)
          case attr: Attribute =>
            Alias(attr, s"col${ columnWithIndex._2 }")(NamedExpression.newExprId)
          case attr => attr
        }
      }
      val version = sparkSession.version
      val newChild: LogicalPlan = if (newChildOutput == childPlan.output) {
        if (version.startsWith("2.1")) {
          CarbonReflectionUtils.getField("child", p).asInstanceOf[LogicalPlan]
        } else if (version.startsWith("2.2")) {
          CarbonReflectionUtils.getField("query", p).asInstanceOf[LogicalPlan]
        } else {
          throw new UnsupportedOperationException(s"Spark version $version is not supported")
        }
      } else {
        Project(newChildOutput, childPlan)
      }

      val overwrite = CarbonReflectionUtils.getOverWriteOption("overwrite", p)

      p.copy(table = convertToLogicalRelation(relation, new CarbonFileFormat, sparkSession))
    } else {
      CarbonException.analysisException(
        "Cannot insert into target table because number of columns mismatch")
    }
  }

  private def convertToLogicalRelation(
      relation: LogicalRelation,
      defaultSource: FileFormat,
      sparkSession: SparkSession): LogicalRelation = {
    if (relation.catalogTable.isDefined) {
      val catalogTable = relation.catalogTable.get
      val table = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
      if (table.isPartitionTable) {
        val metastoreSchema = StructType.fromAttributes(relation.output)
        val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions
        val catalog = new CatalogFileIndex(
          sparkSession, catalogTable, relation.relation.sizeInBytes)
        if (lazyPruningEnabled) {
          catalog
        } else {
          catalog.filterPartitions(Nil) // materialize all the partitions in memory
        }
        val partitionSchema = StructType.fromAttributes(table.getPartitionInfo(table.getTableName)
          .getColumnSchemaList.asScala.map(f=>
          relation.output.find(_.name.equalsIgnoreCase(f.getColumnName))).map(_.get))


        val dataSchema =
          StructType(metastoreSchema
            .filterNot(field => partitionSchema.contains(field.name)))

        val hdfsRelation = HadoopFsRelation(
          location = catalog,
          partitionSchema = partitionSchema,
          dataSchema = dataSchema,
          bucketSpec = catalogTable.bucketSpec,
          fileFormat = defaultSource,
          options = catalogTable.storage.properties)(sparkSession = sparkSession)

        val created = LogicalRelation(hdfsRelation, catalogTable = Some(catalogTable))
        created
      } else {
        relation
      }
    } else {
      relation
    }
  }

  /**
   * Transform the logical plan with average(col1) aggregation type to sum(col1) and count(col1).
   *
   * @param logicalPlan
   * @return
   */
  private def transformAggregatePlan(logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan transform {
      case aggregate@Aggregate(_, aExp, _) =>
        val newExpressions = aExp.flatMap {
          case alias@Alias(attrExpression: AggregateExpression, _) =>
            attrExpression.aggregateFunction match {
              case Average(attr: AttributeReference) =>
                Seq(Alias(attrExpression
                  .copy(aggregateFunction = Sum(attr),
                    resultId = NamedExpression.newExprId), attr.name + "_sum")(),
                  Alias(attrExpression
                    .copy(aggregateFunction = Count(attr),
                      resultId = NamedExpression.newExprId), attr.name + "_count")())
              case Average(cast@MatchCast(attr: AttributeReference, _)) =>
                Seq(Alias(attrExpression
                  .copy(aggregateFunction = Sum(cast),
                    resultId = NamedExpression.newExprId),
                  attr.name + "_sum")(),
                  Alias(attrExpression
                    .copy(aggregateFunction = Count(cast),
                      resultId = NamedExpression.newExprId), attr.name + "_count")())
              case _ => Seq(alias)
            }
          case namedExpr: NamedExpression => Seq(namedExpr)
        }
        aggregate.copy(aggregateExpressions = newExpressions.asInstanceOf[Seq[NamedExpression]])
      case plan: LogicalPlan => plan
    }
  }
}

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

package org.apache.spark.sql.execution.strategy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, GenericRow, NamedExpression}
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}

import org.apache.carbondata.core.readcommitter.ReadCommittedScope
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails}
import org.apache.carbondata.core.util.CarbonUtil

class HBaseFormatBasedHandler extends ExternalFormatHandler {
  /**
   * Generates the RDD using the spark file format.
   */
  override def getRDDForExternalSegments(format: FileFormat,
      loadMetadataDetails: Array[LoadMetadataDetails],
      readCommittedScope: ReadCommittedScope,
      l: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      supportBatch: Boolean): (RDD[InternalRow], Boolean) = {
    val externalSchema = CarbonUtil.getExternalSchemaString(l
      .relation
      .asInstanceOf[CarbonDatasourceHadoopRelation]
      .identifier)
    val projectsString = projects.map(f => f.name)
    val filtersNew = filters.flatMap(DataSourceStrategy.translateFilter)
    val hBaseRelation = HBaseRelation(Map(HBaseTableCatalog.tableCatalog -> externalSchema),
      Some(l.relation.schema))(l.relation.sqlContext)
    val value = hBaseRelation.buildScan(projectsString.toArray, filtersNew.toArray)
      .map(f => new GenericInternalRow(f.asInstanceOf[GenericRow].values).asInstanceOf[InternalRow])
    (value, false)
  }
}

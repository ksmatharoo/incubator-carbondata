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

package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.StructType

class CarbonHBaseRelation(parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType])
  (sqlContext: SQLContext) extends HBaseRelation(parameters, userSpecifiedSchema)(sqlContext) {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new CarbonHBaseTableScanRDD(this,
      requiredColumns.filterNot(f => f.equalsIgnoreCase("rowtimestamp")),
      filters,
      requiredColumns)
  }
}

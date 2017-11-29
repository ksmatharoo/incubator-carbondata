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

package org.apache.carbondata.examples

import java.io.File

object CarbonSessionExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createCarbonSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("WARN")

//    spark.sql("DROP TABLE IF EXISTS carbon_table")
//
//    // Create table
//    spark.sql(
//      s"""
//         | CREATE TABLE carbon_table(
//         | shortField SHORT,
//         | intField INT,
//         | bigintField LONG,
//         | doubleField DOUBLE,
//         | stringField STRING
//         | )
//         | STORED BY 'carbondata'
//       """.stripMargin)
//
//    val rootPath = new File(this.getClass.getResource("/").getPath
//                            + "../../../..").getCanonicalPath
//    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"
//
//    // scalastyle:off
//    spark.sql(
//      s"""
//         | LOAD DATA LOCAL INPATH '$path'
//         | INTO TABLE carbon_table
//         | OPTIONS('HEADER'='true')
//       """.stripMargin)
//    // scalastyle:on
        spark.sql("DROP TABLE IF EXISTS carbon_table1")
    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table1(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE
         | )
         | PARTITIONED BY (stringField STRING)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('PARTITION_TYPE'='LIST',
         | 'LIST_INFO'='spark')
       """.stripMargin)

    spark.sql("insert into carbon_table1 PARTITION(stringField) select * from carbon_table")


    spark.stop()
  }

}

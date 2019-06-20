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

package org.apache.carbondata.spark.testsuite.sdk

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.{CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.sdk.file.{ArrowCarbonReader, CarbonReader, CarbonSchemaReader}

class TestSDKWithTransactionalTable extends QueryTest with BeforeAndAfterAll {
  var filePath: String = _

  def buildTestData() = {
    filePath = s"${integrationPath}/spark-common-test/target/big.csv"
    val file = new File(filePath)
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write("c1,c2,c3, c4, c5, c6, c7, c8, c9, c10")
    writer.newLine()
    for(i <- 0 until 10) {
      writer.write("a" + i%1000 + "," +
                   "b" + i%1000 + "," +
                   "c" + i%1000 + "," +
                   "d" + i%1000 + "," +
                   "e" + i%1000 + "," +
                   "f" + i%1000 + "," +
                   i%1000 + "," +
                   i%1000 + "," +
                   i%1000 + "," +
                   i%1000 + "\n")
      if ( i % 10000 == 0) {
        writer.flush()
      }
    }
    writer.close()
  }

  def dropTable() = {
    sql("DROP TABLE IF EXISTS carbon_load1")
    sql("DROP TABLE IF EXISTS train")
    sql("DROP TABLE IF EXISTS test")
  }

  override def beforeAll {
    dropTable
    buildTestData
  }
  
  test("test sdk with transactional table") {

    sql(
      """
        | CREATE TABLE carbon_load1(c1 string, c2 string, c3 string, c4 string, c5 string,
        | c6 string, c7 int, c8 int, c9 int, c10 int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load1 ")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load1 ")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load1 ")


    val table = CarbonEnv.getCarbonTable(None, "carbon_load1")(sqlContext.sparkSession)

    val value:ArrowCarbonReader[Array[Object]] =
      CarbonReader.builder(table.getTablePath, table.getTableName).buildArrowReader()

    val schema = CarbonSchemaReader.readSchema(table.getTablePath)

    val l = value.readArrowBatchAddress(schema)
    println(l)
    var count = 0
    while(value.hasNext) {
      value.readNextRow()
      count += 1
    }
    value.close()
    checkAnswer(sql("select count(*) from carbon_load1"), Seq(Row(count)))
  }

  test("test load with binary data") {
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS train (
         |    id int,
         |    digit int,
         |    image binary)
         | STORED BY 'carbondata'
             """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '/home/root1/binarymnisttrain.csv'
         | INTO TABLE train
         | OPTIONS('header'='false','DELIMITER'=',','binary_decoder'='baSe64')
             """.stripMargin)
    sql("select count(*) from train").show()

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS test (
         |    id int,
         |    digit int,
         |    image binary)
         | STORED BY 'carbondata'
             """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '/home/root1/binarymnisttest.csv'
         | INTO TABLE test
         | OPTIONS('header'='false','DELIMITER'=',','binary_decoder'='baSe64')
             """.stripMargin)
    sql("select count(*) from test").show()
  }


  override def afterAll {
//    dropTable
    new File(filePath).delete()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
        CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
  }
}


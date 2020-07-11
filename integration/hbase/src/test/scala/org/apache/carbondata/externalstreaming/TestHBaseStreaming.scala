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

package org.apache.carbondata.externalstreaming

import scala.collection.JavaConverters._

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.execution.command.management.CarbonAddExternalStreamingSegmentCommand
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog, HandoffHbaseSegmentCommand, SparkHBaseConf}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll


class TestHBaseStreaming extends QueryTest with BeforeAndAfterAll {
  var htu: HBaseTestingUtility = _
  var loadTimestamp:Long = 0
  var hBaseConfPath:String = _
  val writeCat =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"int"}
       |}
       |}""".stripMargin

  val writeCatTimestamp =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleTable1", "tableCoder":"PrimitiveType"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"int"}
       |}
       |}""".stripMargin

  val catWithTimestamp =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"int"}
       |}
       |}""".stripMargin

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS source")
    sql("DROP TABLE IF EXISTS sourceWithTimestamp")
    val data = (0 until 10).map { i =>
      IntKeyRecord(i)
    }
    htu = new HBaseTestingUtility()
    htu.startMiniCluster(1)
    SparkHBaseConf.conf = htu.getConfiguration
    import sqlContext.implicits._
    hBaseConfPath = s"$integrationPath/hbase/src/test/resources/hbase-site-local.xml"

    val shcExampleTableOption = Map(HBaseTableCatalog.tableCatalog -> writeCat,
      HBaseTableCatalog.newTable -> "5", HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath)
    sqlContext.sparkContext.parallelize(data).toDF.write.options(shcExampleTableOption)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    sql("DROP TABLE IF EXISTS source")
    sql(
      "create table source(col0 int, col1 String, col2 int) stored as carbondata")
    var options = Map("format" -> "HBase")
    options = options + ("querySchema" -> writeCat)
    CarbonAddExternalStreamingSegmentCommand(Some("default"), "source", options).processMetadata(
      sqlContext.sparkSession)

    loadTimestamp = System.currentTimeMillis()
    val shcExampleTable1Option = Map(HBaseTableCatalog.tableCatalog -> writeCat,
      HBaseTableCatalog.newTable -> "5",
      HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath
    )

    sqlContext.sparkContext.parallelize(data).toDF.write.options(shcExampleTable1Option)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    sql("DROP TABLE IF EXISTS sourceWithTimestamp")
    sql(
      "create table sourceWithTimestamp(col0 int, col1 String, col2 int, rowtimestamp LONG) " +
      "stored as carbondata")
    var optionsNew = Map("format" -> "HBase")
    optionsNew = optionsNew + ("querySchema" -> catWithTimestamp)
    CarbonAddExternalStreamingSegmentCommand(Some("default"),
      "sourceWithTimestamp",
      optionsNew).processMetadata(
      sqlContext.sparkSession)
  }

  test("test Full Scan Query") {
    val frame = withCatalog(writeCat)
    checkAnswer(sql("select * from source"), frame)
  }

  test("test Filter Scan Query") {
    val frame = withCatalog(writeCat)
    frame.filter("col0=3")
    checkAnswer(sql("select * from source where col0=3"), frame.filter("col0=3"))
  }

  test("test Full Scan Query with timestamp") {
    val rows = sql("select * from sourceWithTimestamp").collectAsList().asScala
    assert(rows.exists(row => row.get(3).isInstanceOf[Long]))
  }

  test("test handoff segment") {
    val prevRows = sql("select * from sourceWithTimestamp").collect()
    HandoffHbaseSegmentCommand(None, "sourceWithTimestamp", Option.empty, 0, false).run(sqlContext.sparkSession)
    checkAnswer(sql("select * from sourceWithTimestamp"), prevRows)
    val data = (10 until 20).map { i =>
      IntKeyRecord(i)
    }
    val shcExampleTable1Option = Map(HBaseTableCatalog.tableCatalog -> writeCat,
      HBaseTableCatalog.newTable -> "5",
      HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath
    )
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(data).toDF.write.options(shcExampleTable1Option)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    val rows = sql("select * from sourceWithTimestamp").collectAsList()
    assert(rows.size() == 20)
    assert(sql("select * from sourceWithTimestamp where segmentid(1)").collectAsList().size() == 10)
    assert(sql("select * from sourceWithTimestamp where segmentid(2)").collectAsList().size() == 10)
  }

  test("test Full Scan Query with Hbase and carbon segment") {
    sql("insert into table source values(100,'vishal',10)")
    assert(sql("select * from source where col0= '1'").collectAsList().size()==1)
    assert(sql("select * from source where segmentid(1)").collectAsList().size()==1)
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS source")
    sql("DROP TABLE IF EXISTS sourceWithTimestamp")
    htu.shutdownMiniCluster()
  }
}
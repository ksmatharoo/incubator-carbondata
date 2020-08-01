package org.apache.carbondata.externalstreaming

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.spark.sql.execution.command.management.CarbonAddExternalStreamingSegmentCommand
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog, HandoffHbaseSegmentCommand, SparkHBaseConf}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestAllDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  var htu: HBaseTestingUtility = _
  var hBaseConfPath:String = _

  val writeCatTimestamp =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleTable1", "tableCoder":"PrimitiveType"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"bigint"},
       |"col3":{"cf":"cf2", "col":"col3", "type":"float"},
       |"col4":{"cf":"cf2", "col":"col4", "type":"string"},
       |"col5":{"cf":"cf2", "col":"col5", "type":"double"},
       |"col6":{"cf":"cf2", "col":"col6", "type":"double"},
       |"col7":{"cf":"cf2", "col":"col7", "type":"boolean"},
       |"col8":{"cf":"cf2", "col":"col8", "type":"short"},
       |"col9":{"cf":"cf2", "col":"col9", "type":"bigint"},
       |"col10":{"cf":"cf2", "col":"col10", "type":"double"}
       |}
       |}""".stripMargin


//  val writeCatTimestampAll =
//    s"""{
//       |"table":{"namespace":"default", "name":"shcExampleTable1", "tableCoder":"PrimitiveType"},
//       |"rowkey":"key",
//       |"columns":{
//       |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
//       |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
//       |"col2":{"cf":"cf2", "col":"col2", "type":"bigint"},
//       |"col3":{"cf":"cf2", "col":"col3", "type":"float"},
//       |"col4":{"cf":"cf2", "col":"col4", "type":"date"},
//       |"col5":{"cf":"cf2", "col":"col5", "type":"double"},
//       |"col6":{"cf":"cf2", "col":"col6", "type":"double"},
//       |"col7":{"cf":"cf2", "col":"col7", "type":"boolean"},
//       |"col8":{"cf":"cf2", "col":"col8", "type":"short"},
//       |"col9":{"cf":"cf2", "col":"col9", "type":"timestamp"},
//       |"col10":{"cf":"cf2", "col":"col10", "type":"Decimal(10,2)"}
//       |}
//       |}""".stripMargin

  override def beforeAll: Unit = {
    htu = new HBaseTestingUtility()
    htu.startMiniCluster(1)
    SparkHBaseConf.conf = htu.getConfiguration
    import sqlContext.implicits._
    hBaseConfPath = s"$integrationPath/hbase/src/test/resources/hbase-site-local.xml"

    val data = (0 until 10).map { i =>
      MultiDataTypeKeyRecordGenerator(i)
    }
    val shcExampleTable1Option = Map(HBaseTableCatalog.tableCatalog -> writeCatTimestamp,
      HBaseTableCatalog.newTable -> "5",
      HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath
    )
    sqlContext.sparkContext.parallelize(data).toDF.write.options(shcExampleTable1Option)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    sql("DROP TABLE IF EXISTS alldatatype")
    sql(
      "create table alldatatype(col0 int, col1 String, col2 long, col3 float, col4 String, col5 double, col6 double," +
      "col7 boolean, col8 short, col9 long, col10 double) " +
      "stored as carbondata")


//    sql(
//      "create table alldatatype(col0 int, col1 String, col2 long, col3 float, col4 date, col5 double, col6 double," +
//      "col7 boolean, col8 short, col9 timestamp, col10 Decimal(10,2)) " +
//      "stored as carbondata")
    val optionsNew = Map("format" -> "HBase")
    CarbonAddExternalStreamingSegmentCommand(Some("default"),
      "alldatatype",writeCatTimestamp, None,
      optionsNew).processMetadata(
      sqlContext.sparkSession)
  }

  test("test handoff segment") {
    val prevRows = sql("select * from alldatatype").collect()
    HandoffHbaseSegmentCommand(None, "alldatatype", Option.empty, 0, false).run(sqlContext.sparkSession)
    checkAnswer(sql("select * from alldatatype"), prevRows)
    val data = (10 until 20).map { i =>
      MultiDataTypeKeyRecordGenerator(i)
    }
    val shcExampleTable1Option = Map(HBaseTableCatalog.tableCatalog -> writeCatTimestamp,
      HBaseTableCatalog.newTable -> "5",
      HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath
    )
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(data).toDF.write.options(shcExampleTable1Option)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    val rows = sql("select * from alldatatype").collectAsList()
    assert(rows.size() == 20)
    assert(sql("select * from alldatatype where segmentid(1)").collectAsList().size() == 10)
    assert(sql("select * from alldatatype where segmentid(2)").collectAsList().size() == 10)
  }

}

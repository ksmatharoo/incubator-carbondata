package org.apache.spark.carbondata.hbasestreaming

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.command.management.CarbonAddExternalStreamingSegmentCommand
import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog, SparkHBaseConf}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestHBaseStreaming extends QueryTest with BeforeAndAfterAll {
  var htu: HBaseTestingUtility = _

  val cat =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"int"}
       |}
       |}""".stripMargin

  case class IntKeyRecord(
      col0: Integer,
      col1: String,
      col2: Int)

  object IntKeyRecord {
    def apply(i: Int): IntKeyRecord = {
      IntKeyRecord(if (i % 2 == 0) {
        i
      } else {
        -i
      },
        s"String$i extra",
        i)
    }
  }

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  override def beforeAll: Unit = {
    //    val hBaseConfPath = s"/examples/spark/src/main/resources/hbase-site-local.xml"
    val data = (0 until 10).map { i =>
      IntKeyRecord(i)
    }
    htu = new HBaseTestingUtility()
    htu.startMiniCluster(1)
    confSparkSession(SparkHBaseConf.testConf + "=true")
    SparkHBaseConf.conf = htu.getConfiguration
    import sqlContext.implicits._
    val stringToString = Map(HBaseTableCatalog.tableCatalog -> cat,
      HBaseTableCatalog.newTable -> "5")
    sqlContext.sparkContext.parallelize(data).toDF.write.options(stringToString)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    sql("DROP TABLE IF EXISTS source")
    sql("create table source(col0 int, col1, String, col2 int) stored as carbondata")
    var a = Map("format" -> "HBase")
    a = a + ("segmentSchema" -> cat)
    CarbonAddExternalStreamingSegmentCommand(Some("default"), "source1", a).processMetadata(
      sqlContext.sparkSession)
  }

  test("test Full Scan Query") {
    val frame = withCatalog(cat)
    frame.show()
    checkAnswer(sql("select * from source"), frame)
  }

  test("test Filter Scan Query") {
    val frame = withCatalog(cat)
    frame.filter("col0=-3")
    checkAnswer(sql("select * from source1 where col0=-3"), frame.filter("col0=-3"))
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS source")
    htu.shutdownMiniCluster()
  }
}

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

package org.apache.carbondata.cluster.sdv.tpch

import java.io.{BufferedReader, File, FileReader, FileWriter}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.test.TestQueryExecutor.integrationPath
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * TPCH Performance test.
 */
class TpchPerformanceTestCase1 extends QueryTest with BeforeAndAfterAll  {



  val (testType, testMessage) = {
    val str = System.getProperty("tpch.test.type")
    if (str == null) {
      ("default", "Both Carbon and Parquet sorted while loading")
    } else {
      str match {
        case "nosort" =>
          ("nosort", "Both Carbon and Parquet are not sorted while loading")
        case "globalsort" =>
          ("globalsort", "Carbon is global sorted and Parquet is sorted")
        case _ =>
          ("default", "Both Carbon and Parquet sorted while loading")
      }
    }
  }

  override def beforeAll() = {
    CarbonProperties.getInstance().
      addProperty("carbon.number.of.cores.while.loading", "6").
      addProperty("enable.unsafe.sort", "true").
      addProperty("offheap.sort.chunk.size.inmb", "128").
      addProperty("sort.inmemory.size.inmb", "3072").
      addProperty("carbon.unsafe.working.memory.in.mb", "3072").
      addProperty("carbon.enable.vector.reader", "true").
      addProperty("enable.unsafe.in.query.processing", "false").
      addProperty("carbon.blockletgroup.size.in.mb", "128").
      addProperty("enable.query.statistics", "true")
  }

  var database = "tpchcarbon"

  var srcDatabase = "tpchhive"

  val carbonLoadTime = new util.LinkedHashMap[String, LoadResult]()
  val carbonQueryTime = new util.LinkedHashMap[String, Result]()
  val parquetLoadTime = new util.LinkedHashMap[String, LoadResult]()
  val rawDataSize = new util.LinkedHashMap[String, Double]()
  val parquetQueryTime = new util.LinkedHashMap[String, Result]()

  val tpchPath = TestQueryExecutor.tpchDataPath

  case class Result(rowCount:Int, time: Double)

  case class LoadResult(size: Double, time: Double)

  val queries = Seq(
    ("Q1", "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date('1998-09-02') group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"),

    ("Q2", "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier,nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' ) order by s_acctbal desc, n_name, s_name, p_partkey limit 100"),

    ("Q3", "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-15') and l_shipdate > date('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10"),

    ("Q4", "select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date('1993-07-01') and o_orderdate < date('1993-10-01') and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority"),

    ("Q5", "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date('1994-01-01') and o_orderdate < date('1995-01-01') group by n_name order by revenue desc"),

    ("Q6", "select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date('1994-01-01') and l_shipdate < date('1995-01-01') and l_discount between 0.05 and 0.07 and l_quantity < 24"),

    ("Q7", "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier,lineitem,orders,customer,nation n1,nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') ) and l_shipdate between date('1995-01-01') and date('1996-12-31') ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year"),

    ("Q8", "select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from (select year(o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from part,supplier,lineitem,orders,customer,nation n1,nation n2,region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between date('1995-01-01') and date('1996-12-31') and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year"),

    ("Q9", "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, year(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%' ) as profit group by nation, o_year order by nation, o_year desc"),

    ("Q10", "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date('1993-10-01') and o_orderdate < date('1994-01-01') and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc limit 20"),

    ("Q11", "select ps_partkey, sum(ps_supplycost * ps_availqty) as value from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001000000 s from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' ) order by value desc"),

    ("Q12", "select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date('1994-01-01') and l_receiptdate < date('1995-01-01') group by l_shipmode order by l_shipmode"),

    ("Q13", "select c_count, count(*) as custdist from (select c_custkey, count(o_orderkey) as c_count from customer left outer join orders on ( c_custkey = o_custkey and o_comment not like '%special%requests%' ) group by c_custkey ) as c_orders group by c_count order by custdist desc, c_count desc"),

    ("Q14", "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= date('1995-09-01') and l_shipdate < date('1995-10-01')"),

    ("Q15", "create view revenue (supplier_no, total_revenue) as select l_suppkey, sum(l_extendedprice * (1 - l_discount)) from lineitem where l_shipdate >= date('1996-01-01') and l_shipdate < date('1996-04-01') group by l_suppkey"),

    ("Q15", "select s_suppkey, s_name, s_address, s_phone, total_revenue from supplier,revenue where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from revenue ) order by s_suppkey"),

    ("Q15", "drop view revenue"),

    ("Q16", "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from partsupp, part where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in ( select s_suppkey from supplier where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size"),

    ("Q17", "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem,part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < ( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey )"),

    ("Q18", "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate"),

    ("Q19", "select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' )"),

    ("Q20", "select s_name, s_address from supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like 'forest%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date('1994-01-01') and l_shipdate < date('1995-01-01') ) ) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name"),

    ("Q21", "select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name"),

    ("Q22", "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone,1 ,2) as cntrycode, c_acctbal from customer where substring(c_phone ,1,2) in ('13','31','23','29','30','18','17') and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring(c_phone,1,2) in ('13', '31', '23', '29', '30', '18', '17') ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode")
  )

  val createCarbonQueries = readCreateSQL(getCreateFile(true))

  val createParquetQueries = readCreateSQL(getCreateFile(false))

  val createHiveQueries = readCreateSQL("tpch_hive_create.sql")

  val loadHiveQueries = Seq(

    ("LINEITEM", s"""load data LOCAL inpath "$tpchPath/lineitem.tbl" OVERWRITE into table lineitem""".stripMargin),

    ("SUPPLIER", s"""load data LOCAL inpath "$tpchPath/supplier.tbl" OVERWRITE into table SUPPLIER""".stripMargin),

    ("PARTSUPP", s"""load data LOCAL inpath "$tpchPath/partsupp.tbl" OVERWRITE into table PARTSUPP""".stripMargin),

    ("CUSTOMER", s"""load data LOCAL inpath "$tpchPath/customer.tbl" OVERWRITE into  table CUSTOMER""".stripMargin),

    ("NATION", s"""load data LOCAL inpath "$tpchPath/nation.tbl" OVERWRITE into table NATION""".stripMargin),

    ("REGION", s"""load data LOCAL inpath "$tpchPath/region.tbl" OVERWRITE into table REGION""".stripMargin),

    ("PART", s"""load data LOCAL inpath "$tpchPath/part.tbl" OVERWRITE into table PART""".stripMargin),

    ("ORDERS", s"""load data LOCAL inpath "$tpchPath/orders.tbl" OVERWRITE into table ORDERS""".stripMargin)
  )

  val loadParquetQueries = readLoadSQL(getLoadFile(false))

  val loadCarbonQueries = readLoadSQL(getLoadFile(true))

  def getCreateFile(carbon: Boolean): String = {
    testType match {
      case "default" =>
        if (carbon) {
          "tpch_carbon_create_default.sql"
        } else {
          "tpch_parquet_create_default.sql"
        }
      case "nosort" =>
        if (carbon) {
          "tpch_carbon_create_nosort.sql"
        } else {
          "tpch_parquet_create_default.sql"
        }
      case "globalsort" =>
        if (carbon) {
          "tpch_carbon_create_default.sql"
        } else {
          "tpch_parquet_create_default.sql"
        }
      case _ =>
        if (carbon) {
          "tpch_carbon_create_default.sql"
        } else {
          "tpch_parquet_create_default.sql"
        }
    }
  }

  def getLoadFile(carbon: Boolean): String = {
    testType match {
      case "default" =>
        if (carbon) {
          "tpch_carbon_load_default.sql"
        } else {
          "tpch_parquet_load_default.sql"
        }
      case "nosort" =>
        if (carbon) {
          "tpch_carbon_load_default.sql"
        } else {
          "tpch_parquet_load_nosort.sql"
        }
      case "globalsort" =>
        if (carbon) {
          "tpch_carbon_load_global_sort.sql"
        } else {
          "tpch_parquet_load_default.sql"
        }
      case _ =>
        if (carbon) {
          "tpch_carbon_load_default.sql"
        } else {
          "tpch_parquet_load_default.sql"
        }
    }
  }

  def readCreateSQL(fileName: String): Seq[String] = {
    println("Reading create file: "+fileName)
    val file = new File(s"$integrationPath//spark-common-cluster-test/src/test/resources/$fileName")
    val reader = new BufferedReader(new FileReader(file))
    var str = reader.readLine()
    val buffer = new ArrayBuffer[String]()
    while (str != null) {
      buffer += str
      str = reader.readLine()
    }
    reader.close()
    buffer
  }

  def readLoadSQL(fileName: String): Seq[(String, String)] = {
    println("Reading Load file: "+fileName)
    val file = new File(s"$integrationPath//spark-common-cluster-test/src/test/resources/$fileName")
    val reader = new BufferedReader(new FileReader(file))
    var str1 = reader.readLine()
    var str2 = reader.readLine()
    val buffer = new ArrayBuffer[(String, String)]()
    while (str1 != null && str2 != null) {
      buffer += ((str1.replace("#", ""), str2.replace("$warehouse", warehouse)))
      str1 = reader.readLine()
      str2 = reader.readLine()
    }
    reader.close()
    buffer
  }

  test("load hive queries") {
    sql(s"drop database if exists $srcDatabase cascade").collect()
    sql(s"create database if not exists $srcDatabase").collect()
    sql(s"use $srcDatabase").collect()
    createHiveQueries.foreach {q =>
      sql(q).collect()
    }
    loadHiveQueries.foreach { q =>
      sql(q._2).collect()
      val size = FileFactory.getDirectorySize(warehouse+s"/$srcDatabase.db/${q._1.toLowerCase}")
      rawDataSize.put(q._1, (size/(1024 * 1024)))
    }
  }

  test("test carbon performance") {
    createAndLoadTablesCarbon()
    queries.foreach { q =>
      var rows:Array[Row] = null
      println("Executing Carbon : "+q._1)
      var queryPass = true
      var t= time {
        try {
          rows = sql(q._2).collect()
        } catch {
          case _ => queryPass = false
        }
      }
      var d = carbonQueryTime.get(q._1)
      if (queryPass) {
        if (d != null) {
          d = Result(d.rowCount + rows.length, d.time + t)
        } else {
          d = Result(rows.length, t)
        }
      } else {
        d = Result(0, -1)
      }
      println(d)
      carbonQueryTime.put(q._1, d)
    }
    println(carbonLoadTime)
    println(carbonQueryTime)
  }

  test("test parquet performance") {
    createAndLoadTablesParquet()
    queries.foreach { q =>
      var rows:Array[Row] = null
      println("Executing Parquet : "+q._1)
      var queryPass = true
      var t= time {
        try {
          rows = sql(q._2).collect()
        } catch {
          case _ => queryPass = false
        }
      }
      var d = parquetQueryTime.get(q._1)
      if (queryPass) {
        if (d != null) {
          d = Result(d.rowCount + rows.length, d.time + t)
        } else {
          d = Result(rows.length, t)
        }
      } else {
        d = Result(0, -1)
      }
      println(d)
      parquetQueryTime.put(q._1, d)
    }
    println(parquetLoadTime)
    println(parquetQueryTime)
  }

  test("generate result") {
    generateResult()
  }


  def createAndLoadTablesCarbon(): Unit = {
    database = "tpchcarbon"
    sql(s"drop database if exists $database cascade").collect()
    sql(s"create database if not exists $database").collect()
    sql(s"use $database").collect()
    createCarbonQueries.foreach {q =>
      sql(q).collect()
    }
    sql(s"use $database")
    loadCarbonQueries.foreach {q =>
      println("Loading Carbon : "+q._1)
      var queryPass = true
      var t= time {
        try {
          sql(q._2).collect()
        } catch {
          case _ => queryPass = false
        }
      }
      if (!queryPass) {
        t = -1
      }
      println("Time is "+ t +" and Count of "+q._1)
      sql("select count(*) from "+ q._1 ).show()
      val size = FileFactory.getDirectorySize(warehouse+s"/$database/${q._1.toLowerCase}")
      carbonLoadTime.put(q._1, LoadResult((size/(1024 * 1024)), t))
    }
  }

  def createAndLoadTablesParquet(): Unit = {
    database = "tpchparquet"
    sql(s"drop database if exists $database cascade").collect()
    sql(s"create database if not exists $database").collect()
    sql(s"use $database").collect()
    createParquetQueries.foreach {q =>
      sql(q).collect()
    }
    loadParquetQueries.foreach {q =>
      println("Loading Parquet : "+q._1)
      var queryPass = true
      var t= time {
        try {
          sql(q._2).collect()
        } catch {
          case _ => queryPass = false
        }
      }
      if (!queryPass) {
        t = -1
      }
      println("Time is "+ t +" and Count of "+q._1)
      sql("select count(*) from "+ q._1 ).show()
      val size = FileFactory.getDirectorySize(warehouse+s"/$database.db/${q._1.toLowerCase}")
      parquetLoadTime.put(q._1, LoadResult((size/(1024 * 1024)), t))
    }
  }

  def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }

  def generateResult(): Unit = {
    val builder = new StringBuilder
    builder.append(
      s"""
         |<body>
         |      <h1>TPCH Load Performance</h1>
         |      <h2>$testMessage</h2>
         |      <table border = "1">
         |         <tr style = "color:black; font-size:18px;" bgcolor = "green">
         |            <td >Table Name</td>
         |            <td>Carbon</td>
         |            <td>Carbon Size(MB)</td>
         |	          <td>Parquet</td>
         |	          <td>Parquet Size(MB)</td>
         |	          <td>Raw Data Size(MB)</td>
         |         </tr>
       """)
    carbonLoadTime.asScala.foreach {f =>
      builder.append(s"""<tr style = "color:black; font-size:15px;" bgcolor = "yellow">""")
      builder.append(s"""<td>${f._1}</td>""")
      if (f._2.time < 0) {
        builder.append(s"""<td>FAIL</td>""")
        builder.append(s"""<td>FAIL</td>""")
      } else {
        builder.append(s"""<td>${ f._2.time }</td>""")
        builder.append(s"""<td>${ f._2.size }</td>""")
      }
      if (parquetLoadTime.get(f._1).time < 0) {
        builder.append(s"""<td>FAIL</td>""")
        builder.append(s"""<td>FAIL</td>""")
      } else {
        builder.append(s"""<td>${parquetLoadTime.get(f._1).time}</td>""")
        builder.append(s"""<td>${parquetLoadTime.get(f._1).size}</td>""")
      }
      builder.append(s"""<td>${ rawDataSize.get(f._1) }</td>""")
      builder.append(s"""</tr>""")
    }
    builder.append(s"""</table>""")
    builder.append(
      s"""
         |<h1>TPCH Query Performance</h1>
         |<h2>$testMessage</h2>
         |      <table border = "1">
         |         <tr style = "color:black; font-size:18px;" bgcolor = "green">
         |            <td>Queries</td>
         |            <td>Carbon</td>
         |	          <td>Parquet</td>
         |           <td>Carbon Row Count</td>
         |           <td>Parquet Row Count</td>
         |         </tr>
       """)
    carbonQueryTime.asScala.foreach{ f =>
      builder.append(s"""<tr style = "color:black; font-size:15px;" bgcolor = "yellow">""")
      val result = parquetQueryTime.get(f._1)
      builder.append(s"""<td>${f._1}</td>""")
      if (f._2.time < 0) {
        builder.append(s"""<td bgcolor = "red">FAIL</td>""")
      } else {
        if (f._2.time > result.time) {
          builder.append(s"""<td bgcolor = "red">${ f._2.time }</td>""")
        } else {
          builder.append(s"""<td bgcolor = "green">${ f._2.time }</td>""")
        }
      }
      if (result.time < 0) {
        builder.append(s"""<td>FAIL</td>""")
      } else {
        builder.append(s"""<td>${result.time}</td>""")
      }
      builder.append(s"""<td>${f._2.rowCount}</td>""")
      builder.append(s"""<td>${result.rowCount}</td>""")
      builder.append(s"""</tr>""")
    }
    builder.append(
      s"""
         |</table>
         |
         |</body>
         |</html>
       """.stripMargin)
    val file = new File(TestQueryExecutor.integrationPath+"/spark-common-cluster-test/target/tpch")
    file.mkdirs()
    val writer = new FileWriter(new File(file.getAbsolutePath+"/index.html"))
    writer.write(builder.toString())
    writer.close()
  }

  override def afterAll(): Unit = {
    sql(s"drop database if exists tpchcarbon cascade").collect()
    sql(s"drop database if exists tpchparquet cascade").collect()
//    sql(s"drop database if exists $srcDatabase cascade").collect()
  }
}

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

import java.io.{File, FileWriter}
import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.CarbonProperties

/**
 * TPCH Performance test.
 */
class TpchPerformanceTestCase1 extends QueryTest with BeforeAndAfterAll  {


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

  val carbonLoadTime = new util.LinkedHashMap[String, Double]()
  val carbonQueryTime = new util.LinkedHashMap[String, Result]()
  val parquetLoadTime = new util.LinkedHashMap[String, Double]()
  val parquetQueryTime = new util.LinkedHashMap[String, Result]()

  val tpchPath = TestQueryExecutor.tpchDataPath

  case class Result(rowCount:Int, time: Double)

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

  val createCarbonQueries = Seq(
    s"""create table if not exists SUPPLIER(
      S_COMMENT string,
      S_SUPPKEY string,
      S_NAME string,
      S_ADDRESS string,
      S_NATIONKEY string,
      S_PHONE string,
      S_ACCTBAL double
    ) STORED BY 'org.apache.carbondata.format'
    TBLPROPERTIES ('DICTIONARY_EXCLUDE'='S_COMMENT, S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE' , 'table_blocksize'='300')""".stripMargin,

    s"""create table if not exists PARTSUPP (
      PS_PARTKEY string,
      PS_SUPPKEY  string,
      PS_AVAILQTY  int,
      PS_SUPPLYCOST  double,
      PS_COMMENT  string
    ) STORED BY 'org.apache.carbondata.format'
    TBLPROPERTIES ('DICTIONARY_EXCLUDE'='PS_PARTKEY, PS_SUPPKEY, PS_COMMENT', 'table_blocksize'='300')""".stripMargin,

    s"""create table if not exists CUSTOMER(
      C_MKTSEGMENT string,
      C_NATIONKEY string,
      C_CUSTKEY string,
      C_NAME string,
      C_ADDRESS string,
      C_PHONE string,
      C_ACCTBAL double,
      C_COMMENT string
    ) STORED BY 'org.apache.carbondata.format'
    TBLPROPERTIES ('DICTIONARY_EXCLUDE'='C_CUSTKEY,C_NAME,C_ADDRESS,C_PHONE,C_COMMENT', 'table_blocksize'='300')""".stripMargin,

    s"""create table if not exists NATION (
      N_NAME string,
      N_NATIONKEY string,
      N_REGIONKEY string,
      N_COMMENT  string
    ) STORED BY 'org.apache.carbondata.format'
    TBLPROPERTIES ('DICTIONARY_EXCLUDE'='N_COMMENT', 'table_blocksize'='300')""".stripMargin,

    s"""create table if not exists REGION(
      R_NAME string,
      R_REGIONKEY string,
      R_COMMENT string
    ) STORED BY 'org.apache.carbondata.format'
    TBLPROPERTIES ('DICTIONARY_EXCLUDE'='R_COMMENT', 'table_blocksize'='300')""".stripMargin,

    s"""create table if not exists PART(
      P_BRAND string,
      P_SIZE int,
      P_CONTAINER string,
      P_TYPE string,
      P_PARTKEY string,
      P_NAME string,
      P_MFGR string,
      P_RETAILPRICE double,
      P_COMMENT string
    ) STORED BY 'org.apache.carbondata.format'
    TBLPROPERTIES ('DICTIONARY_INCLUDE'='P_SIZE','DICTIONARY_EXCLUDE'='P_PARTKEY, P_NAME, P_COMMENT', 'table_blocksize'='300')""".stripMargin,

    s"""create table if not exists lineitem(
      L_SHIPDATE date,
      L_SHIPMODE string,
      L_SHIPINSTRUCT string,
      L_RETURNFLAG string,
      L_RECEIPTDATE date,
      L_ORDERKEY string,
      L_PARTKEY string,
      L_SUPPKEY   string,
      L_LINENUMBER int,
      L_QUANTITY double,
      L_EXTENDEDPRICE double,
      L_DISCOUNT double,
      L_TAX double,
      L_LINESTATUS string,
      L_COMMITDATE date,
      L_COMMENT  string
    ) STORED BY 'org.apache.carbondata.format'
    TBLPROPERTIES ('DICTIONARY_EXCLUDE'='L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_COMMENT', 'table_blocksize'='300', 'no_inverted_index'='L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_COMMENT')""".stripMargin,

    s"""create table if not exists ORDERS(
      O_ORDERDATE date,
      O_ORDERPRIORITY string,
      O_ORDERSTATUS string,
      O_ORDERKEY string,
      O_CUSTKEY string,
      O_TOTALPRICE double,
      O_CLERK string,
      O_SHIPPRIORITY int,
      O_COMMENT string
    ) STORED BY 'org.apache.carbondata.format'
    TBLPROPERTIES ('DICTIONARY_EXCLUDE'='O_ORDERKEY, O_CUSTKEY, O_CLERK, O_COMMENT', 'table_blocksize'='300','no_inverted_index'='O_ORDERKEY, O_CUSTKEY, O_CLERK, O_COMMENT')""".stripMargin
  )

  val createParquetQueries = Seq(
    s"""create table if not exists SUPPLIER(
      S_SUPPKEY string,
      S_NAME string,
      S_ADDRESS string,
      S_NATIONKEY string,
      S_PHONE string,
      S_ACCTBAL double,
      S_COMMENT string
    )
    stored as parquet
    tblproperties("parquet.compression"="snappy")""".stripMargin,

    s"""create table if not exists PARTSUPP (
      PS_PARTKEY string,
      PS_SUPPKEY  string,
      PS_AVAILQTY  int,
      PS_SUPPLYCOST  double,
      PS_COMMENT  string
    )
    stored as parquet
    tblproperties("parquet.compression"="snappy")""".stripMargin,

    s"""create table if not exists CUSTOMER(
      c_mktsegment string,
      C_CUSTKEY string,
      C_NAME string,
      C_ADDRESS string,
      C_NATIONKEY string,
      C_PHONE string,
      C_ACCTBAL double,
      C_COMMENT string
    )
    stored as parquet
    tblproperties("parquet.compression"="snappy")""".stripMargin,


    s"""create table if not exists NATION (
      N_NATIONKEY string,
      N_REGIONKEY string,
      N_COMMENT  string
    )
    partitioned by (n_name string)
    stored as parquet
    tblproperties("parquet.compression"="snappy")""".stripMargin,

    s"""create table if not exists REGION(
      R_REGIONKEY string,
      R_COMMENT string
    )
    partitioned by (r_name string)
    stored as parquet
    tblproperties("parquet.compression"="snappy")""".stripMargin,

    s"""create table if not exists PART(
      P_BRAND string,
      P_PARTKEY string,
      P_NAME string,
      P_MFGR string,
      P_TYPE string,
      P_SIZE int,
      P_CONTAINER string,
      P_RETAILPRICE double,
      P_COMMENT string
    )
    stored as parquet
    tblproperties("parquet.compression"="snappy")""".stripMargin,

    s"""create table if not exists lineitem(
      L_SHIPDATE date,
      L_ORDERKEY string,
      L_PARTKEY string,
      L_SUPPKEY   string,
      L_LINENUMBER int,
      L_QUANTITY double,
      L_EXTENDEDPRICE double,
      L_DISCOUNT double,
      L_TAX double,
      L_RETURNFLAG string,
      L_LINESTATUS string,
      L_COMMITDATE date,
      L_RECEIPTDATE date,
      L_SHIPINSTRUCT string,
      L_SHIPMODE string,
      L_COMMENT  string
    )
    stored as parquet
    tblproperties("parquet.compression"="snappy")""".stripMargin,

    s"""create table if not exists ORDERS(
      O_ORDERDATE date,
      O_ORDERKEY string,
      O_CUSTKEY string,
      O_ORDERSTATUS string,
      O_TOTALPRICE double,
      O_ORDERPRIORITY string,
      O_CLERK string,
      O_SHIPPRIORITY int,
      O_COMMENT  string
    )
    stored as parquet
    tblproperties("parquet.compression"="snappy")""".stripMargin
  )

  val createHiveQueries = Seq(

    s"""create table if not exists SUPPLIER(
      S_SUPPKEY string,
      S_NAME string,
      S_ADDRESS string,
      S_NATIONKEY string,
      S_PHONE string,
      S_ACCTBAL double,
      S_COMMENT string
    ) row format delimited fields terminated by '|'""".stripMargin,

    s"""create table if not exists PARTSUPP (
      PS_PARTKEY string,
      PS_SUPPKEY  string,
      PS_AVAILQTY  int,
      PS_SUPPLYCOST  double,
      PS_COMMENT  string
    ) row format delimited fields terminated by '|'""".stripMargin,

    s"""create table if not exists CUSTOMER(
      C_CUSTKEY string,
      C_NAME string,
      C_ADDRESS string,
      C_NATIONKEY string,
      C_PHONE string,
      C_ACCTBAL double,
      C_MKTSEGMENT string,
      C_COMMENT string
    ) row format delimited fields terminated by '|'""".stripMargin,


    s"""create table if not exists NATION (
      N_NATIONKEY string,
      N_NAME string,
      N_REGIONKEY string,
      N_COMMENT  string
    ) row format delimited fields terminated by '|'""".stripMargin,

    s"""create table if not exists REGION(
      R_REGIONKEY string,
      R_NAME string,
      R_COMMENT string
    ) row format delimited fields terminated by '|'""".stripMargin,

    s"""create table if not exists PART(
      P_PARTKEY string,
      P_NAME string,
      P_MFGR string,
      P_BRAND string,
      P_TYPE string,
      P_SIZE int,
      P_CONTAINER string,
      P_RETAILPRICE double,
      P_COMMENT string
    ) row format delimited fields terminated by '|'""".stripMargin,

    s"""create table if not exists lineitem(
      L_ORDERKEY string,
      L_PARTKEY string,
      L_SUPPKEY   string,
      L_LINENUMBER int,
      L_QUANTITY double,
      L_EXTENDEDPRICE double,
      L_DISCOUNT double,
      L_TAX double,
      L_RETURNFLAG string,
      L_LINESTATUS string,
      L_SHIPDATE date,
      L_COMMITDATE date,
      L_RECEIPTDATE date,
      L_SHIPINSTRUCT string,
      L_SHIPMODE string,
      L_COMMENT  string
    ) row format delimited fields terminated by '|'""".stripMargin,

    s"""create table if not exists ORDERS(
      O_ORDERKEY string,
      O_CUSTKEY string,
      O_ORDERSTATUS string,
      O_TOTALPRICE double,
      O_ORDERDATE date,
      O_ORDERPRIORITY string,
      O_CLERK string,
      O_SHIPPRIORITY int,
      O_COMMENT  string
    ) row format delimited fields terminated by '|'""".stripMargin
  )
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

  val loadParquetQueries = Seq(

    ("SUPPLIER",s"""insert into table supplier select * from $srcDatabase.supplier""".stripMargin),

    ("ORDERS",s"""insert into table ORDERS select
      O_ORDERDATE,
      O_ORDERKEY,
      O_CUSTKEY,
      O_ORDERSTATUS,
      O_TOTALPRICE,
      O_ORDERPRIORITY,
      O_CLERK,
      O_SHIPPRIORITY,
      O_COMMENT from $srcDatabase.ORDERS sort by O_ORDERDATE,O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS""".stripMargin),

    ("LINEITEM", s"""insert into table lineitem select L_SHIPDATE,L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT from $srcDatabase.lineitem sort BY l_shipdate,L_ORDERKEY,L_PARTKEY,L_SUPPKEY""".stripMargin),

    ("PARTSUPP",s"""insert into table PARTSUPP select * from $srcDatabase.partsupp sort by PS_PARTKEY""".stripMargin),

    ("NATION",s"""insert into table nation partition(N_NAME) select N_NATIONKEY,N_REGIONKEY,N_COMMENT,n_name from $srcDatabase.nation""".stripMargin),

    ("REGION",s"""insert into table REGION partition(r_name) select
    R_REGIONKEY,
    R_COMMENT,
    r_name
    from $srcDatabase.region""".stripMargin),

    ("PART",s"""insert into TABLE PART
    select
    P_BRAND,
    P_PARTKEY,
    P_NAME,
    P_MFGR,
    P_TYPE,
    P_SIZE,
    P_CONTAINER,
    P_RETAILPRICE,
    P_COMMENT
    from $srcDatabase.part sort by P_BRAND,P_PARTKEY,P_NAME """.stripMargin),

    ("CUSTOMER",s"""insert into table CUSTOMER select
    C_MKTSEGMENT,
    C_CUSTKEY,
    C_NAME,
    C_ADDRESS,
    C_NATIONKEY,
    C_PHONE,
    C_ACCTBAL,
    C_COMMENT
    from $srcDatabase.customer order by C_MKTSEGMENT """.stripMargin)
  )

  val loadCarbonQueries = Seq(

    ("LINEITEM", s"""load data inpath "$warehouse/$srcDatabase.db/lineitem/lineitem.tbl" into table lineitem options('DELIMITER'='|','FILEHEADER'='L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT')""".stripMargin),

    ("SUPPLIER", s"""load data inpath "$warehouse/$srcDatabase.db/supplier/supplier.tbl" into table SUPPLIER options('DELIMITER'='|','FILEHEADER'='S_SUPPKEY,S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT')""".stripMargin),

    ("PARTSUPP", s"""load data inpath "$warehouse/$srcDatabase.db/partsupp/partsupp.tbl" into table PARTSUPP options('DELIMITER'='|','FILEHEADER'='PS_PARTKEY,PS_SUPPKEY,PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT')""".stripMargin),

    ("CUSTOMER", s"""load data inpath "$warehouse/$srcDatabase.db/customer/customer.tbl" into  table CUSTOMER options('DELIMITER'='|','FILEHEADER'='C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT')""".stripMargin),

    ("NATION", s"""load data inpath "$warehouse/$srcDatabase.db/nation/nation.tbl" into table NATION options('DELIMITER'='|','FILEHEADER'='N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT')""".stripMargin),

    ("REGION", s"""load data inpath "$warehouse/$srcDatabase.db/region/region.tbl" into table REGION options('DELIMITER'='|','FILEHEADER'='R_REGIONKEY,R_NAME,R_COMMENT')""".stripMargin),

    ("PART", s"""load data inpath "$warehouse/$srcDatabase.db/part/part.tbl" into table PART options('DELIMITER'='|','FILEHEADER'='P_PARTKEY,P_NAME,P_MFGR,P_BRAND,P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMENT')""".stripMargin),

    ("ORDERS", s"""load data inpath "$warehouse/$srcDatabase.db/orders/orders.tbl" into table ORDERS options('DELIMITER'='|','FILEHEADER'='O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT')""".stripMargin)
  )

  test("load hive queries") {
    sql(s"drop database if exists $srcDatabase cascade").collect()
    sql(s"create database if not exists $srcDatabase").collect()
    sql(s"use $srcDatabase").collect()
    createHiveQueries.foreach {q =>
      sql(q).collect()
    }
    loadHiveQueries.foreach { q =>
      sql(q._2).collect()
    }
  }

  test("test carbon performance") {
    createAndLoadTablesCarbon()
    queries.foreach { q =>
      var rows:Array[Row] = null
      println("Executing Carbon : "+q._1)
      var t= time {
        rows = sql(q._2).collect()
      }
      var d = carbonQueryTime.get(q._1)
      if (d != null) {
        d = Result(d.rowCount+rows.length, d.time + t)
      } else {
        d = Result(rows.length, t)
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
      var t= time {
        rows = sql(q._2).collect()
      }
      var d = parquetQueryTime.get(q._1)
      if (d != null) {
        d = Result(d.rowCount+rows.length, d.time + t)
      } else {
        d = Result(rows.length, t)
      }
      println(d)
      parquetQueryTime.put(q._1, d)
    }
    println(parquetLoadTime)
    println(parquetQueryTime)
    println("run on hive")
    sql(s"use $srcDatabase").collect()
    var rows:Array[Row] = null
    val t = time {
      rows = sql(
        "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate").collect()
    }
    println(Result(rows.length, t))
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
      val t= time {
        sql(q._2).collect()
      }
      println("Time is "+ t +" and Count of "+q._1)
      sql("select count(*) from "+ q._1 ).show()
      carbonLoadTime.put(q._1, t)
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
      val t= time {
        sql(q._2).collect()
      }
      println("Time is "+ t +" and Count of "+q._1)
      sql("select count(*) from "+ q._1 ).show()
      parquetLoadTime.put(q._1, t)
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
         |      <table border = "1">
         |         <tr style = "color:black; font-size:18px;" bgcolor = "green">
         |            <td >Table Name</td>
         |            <td>Carbon</td>
         |	          <td>Parquet</td>
         |         </tr>
       """)
    carbonLoadTime.asScala.foreach {f =>
      builder.append(s"""<tr style = "color:black; font-size:15px;" bgcolor = "yellow">""")
      builder.append(s"""<td>${f._1}</td>""")
      builder.append(s"""<td>${f._2}</td>""")
      builder.append(s"""<td>${parquetLoadTime.get(f._1)}</td>""")
      builder.append(s"""</tr>""")
    }
    builder.append(s"""</table>""")
    builder.append(
      s"""
         |<h1>TPCH Query Performance</h1>
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
      builder.append(s"""<td>${f._1}</td>""")
      builder.append(s"""<td>${f._2.time}</td>""")
      val result = parquetQueryTime.get(f._1)
      builder.append(s"""<td>${result.time}</td>""")
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

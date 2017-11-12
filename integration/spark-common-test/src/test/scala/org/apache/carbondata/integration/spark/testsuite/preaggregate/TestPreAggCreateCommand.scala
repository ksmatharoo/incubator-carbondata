package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestPreAggCreateCommand extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists PreAggMain")
    sql("drop table if exists PreAggMain1")
    sql("drop table if exists PreAggMain2")
    sql("create table preaggMain (a string, b string, c string) stored by 'carbondata'")
    sql("create table preaggMain1 (a string, b string, c string) stored by 'carbondata' tblProperties('DICTIONARY_INCLUDE' = 'a')")
    sql("create table preaggMain2 (a string, b string, c string) stored by 'carbondata'")
  }


  test("test pre agg create table One") {
    sql("create datamap preagg1 on table PreAggMain using 'preaggregate' as select a,sum(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg1"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg1"), true, "preaggmain_b_sum")
    sql("drop table preagg1")
  }

  test("test pre agg create table Two") {
    sql("create datamap preagg2 on table PreAggMain using 'preaggregate' as select a as a1,sum(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg2"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg2"), true, "preaggmain_b_sum")
    sql("drop table preagg2")
  }

  test("test pre agg create table Three") {
    sql("create datamap preagg3 on table PreAggMain using 'preaggregate' as select a,sum(b) as sum from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg3"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg3"), true, "preaggmain_b_sum")
    sql("drop table preagg3")
  }

  test("test pre agg create table four") {
    sql("create datamap preagg4 on table PreAggMain using 'preaggregate' as select a as a1,sum(b) as sum from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg4"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg4"), true, "preaggmain_b_sum")
    sql("drop table preagg4")
  }


  test("test pre agg create table five") {
    sql("create datamap preagg11 on table PreAggMain1 using 'preaggregate'as select a,sum(b) from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg11"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg11"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED preagg11"), true, "DICTIONARY")
    sql("drop table preagg11")
  }

  test("test pre agg create table six") {
    sql("create datamap preagg12 on table PreAggMain1 using 'preaggregate' as select a as a1,sum(b) from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg12"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg12"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED preagg12"), true, "DICTIONARY")
    sql("drop table preagg12")
  }

  test("test pre agg create table seven") {
    sql("create datamap preagg13 on table PreAggMain1 using 'preaggregate' as select a,sum(b) as sum from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg13"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg13"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED preagg13"), true, "DICTIONARY")
    sql("drop table preagg13")
  }

  test("test pre agg create table eight") {
    sql("create datamap preagg14 on table PreAggMain1 using 'preaggregate' as select a as a1,sum(b) as sum from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg14"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg14"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED preagg14"), true, "DICTIONARY")
    sql("drop table preagg14")
  }


  test("test pre agg create table nine") {
    sql("create datamap preagg15 on table PreAggMain2 using 'preaggregate' as select a,avg(b) from PreAggMain2 group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg15"), true, "preaggmain2_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg15"), true, "preaggmain2_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED preagg15"), true, "preaggmain2_b_count")
    sql("drop table preagg15")
  }

  test("test pre agg create table ten") {
    sql("create datamap preagg16 on table PreAggMain2 using 'preaggregate' as select a as a1,max(b) from PreAggMain2 group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg16"), true, "preaggmain2_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg16"), true, "preaggmain2_b_max")
    sql("drop table preagg16")
  }

  test("test pre agg create table eleven") {
    sql("create datamap preagg17 on table PreAggMain2 using 'preaggregate' as select a,min(b) from PreAggMain2 group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg17"), true, "preaggmain2_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg17"), true, "preaggmain2_b_min")
    sql("drop table preagg17")
  }

  test("test pre agg create table twelve") {
    sql("create datamap preagg18 on table PreAggMain2 using 'preaggregate' as select a as a1,count(b) from PreAggMain2 group by a")
    checkExistence(sql("DESCRIBE FORMATTED preagg18"), true, "preaggmain2_a")
    checkExistence(sql("DESCRIBE FORMATTED preagg18"), true, "preaggmain2_b_count")
    sql("drop table preagg18")
  }

  test("test pre agg create table thirteen") {
    try {
      sql(
        "create datamap preagg19 on table PreAggMain2 using 'preaggregate' as select a as a1,count(distinct b) from PreAggMain2 group by a")
      assert(false)
    } catch {
      case _: Exception =>
        assert(true)
    }
  }

  test("test pre agg create table fourteen") {
    try {
      sql(
        "create datamap preagg20 on table PreAggMain2 using 'preaggregate' as select a as a1,sum(distinct b) from PreAggMain2 group by a")
      assert(false)
    } catch {
      case _: Exception =>
        assert(true)
    }
  }

  test("test pre agg create table fifteen") {
    try {
      sql(
        "create datamap preagg21 on table PreAggMain2 using 'preaggregate' as select a as a1,sum(b) from PreAggMain2 where a='vishal' group by a")
      assert(false)
    } catch {
      case _: Exception =>
        assert(true)
    }
  }


  override def afterAll {
    sql("drop table if exists PreAggMain")
    sql("drop table if exists PreAggMain1")
    sql("drop table if exists PreAggMain2")
  }
}

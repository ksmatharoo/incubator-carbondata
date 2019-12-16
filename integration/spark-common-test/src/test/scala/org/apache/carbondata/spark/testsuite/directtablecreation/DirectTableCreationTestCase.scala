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

package org.apache.carbondata.spark.testsuite.directtablecreation

import java.sql.Date

import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.mutation.merge._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for join query with orderby and limit
 */

class DirectTableCreationTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

  }



  test("check the ccd ") {
    sql("drop table if exists target")

    val initframe = sqlContext.sparkSession.createDataFrame(Seq(
      Row("a", "0"),
      Row("b", "1"),
      Row("c", "2"),
      Row("d", "3")
    ).asJava, StructType(Seq(StructField("key", StringType), StructField("value", StringType))))

    initframe.write
      .format("carbondata")
      .option("tableName", "target")
      .mode(SaveMode.Overwrite)
      .save()
    val target = sqlContext.read.format("carbondata").option("tableName", "target").load()
    var ccd =
      sqlContext.sparkSession.createDataFrame(Seq(
        Row("a", "10", false,  0),
        Row("a", null, true, 1),   // a was updated and then deleted
        Row("b", null, true, 2),   // b was just deleted once
        Row("c", null, true, 3),   // c was deleted and then updated twice
        Row("c", "20", false, 4),
        Row("c", "200", false, 5),
        Row("e", "100", false, 6)  // new key
      ).asJava,
        StructType(Seq(StructField("key", StringType),
          StructField("newValue", StringType),
          StructField("deleted", BooleanType), StructField("time", IntegerType))))
    ccd.createOrReplaceTempView("changes")

    ccd = sql("SELECT key, latest.newValue as newValue, latest.deleted as deleted FROM ( SELECT key, max(struct(time, newValue, deleted)) as latest FROM changes GROUP BY key)")

    val updateMap = Map("key" -> "B.key", "value" -> "B.newValue").asInstanceOf[Map[Any, Any]]

    val insertMap = Map("key" -> "B.key", "value" -> "B.newValue").asInstanceOf[Map[Any, Any]]

    target.as("A").merge(ccd.as("B"), "A.key=B.key").
      whenMatched("B.deleted=false").
      updateExpr(updateMap).
      whenNotMatched("B.deleted=false").
      insertExpr(insertMap).
      whenMatched("B.deleted=true").
      delete().execute()
    checkAnswer(sql("select count(*) from target"), Seq(Row(3)))
    checkAnswer(sql("select * from target order by key"), Seq(Row("c", "200"), Row("d", "3"), Row("e", "100")))
  }

  override def afterAll {
    sql("drop table if exists order")
  }
}

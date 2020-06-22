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

import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.hbase.types.{SHCDataType, SHCDataTypeFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType}

class CarbonHBaseTableScanRDD(relation: HBaseRelation,
    requiredColumns: Array[String],
    filters: Array[Filter],
    allRequiredColumns: Array[String]) extends HBaseTableScanRDD(relation, requiredColumns, filters) {

  val isTimeStampColumnRequired = allRequiredColumns.filter(f=>f.equalsIgnoreCase("rowtimestamp")).length > 0
  override def buildRow(fields: Seq[Field], result: Result): Row = {
    // TODO This can be done with below code, but every time it will create new row object when timestamp is enabled
//    val row = super.buildRow(fields, result)
//    val finalRow = if (isTimeStampColumnRequired) {
//      Row.fromSeq(row.asInstanceOf[GenericRow].values :+ result.rawCells()(0).getTimestamp)
//    } else {
//      row
//    }
//    finalRow
    val r = result.getRow
    val keySeq = {
      if (relation.isComposite()) {
        relation.catalog.shcTableCoder
          .decodeCompositeRowKey(r, relation.catalog.getRowKey)
      } else {
        val f = relation.catalog.getRowKey.head
        Seq((f, SHCDataTypeFactory.create(f).fromBytes(r))).toMap
      }
    }
    import scala.collection.JavaConverters.mapAsScalaMapConverter
    val scalaMap = result.getMap.asScala
    val valuesSeq = scalaMap.flatMap { case (cf, columns) =>
      val cfName = relation.catalog.shcTableCoder.fromBytes(cf).toString
      val cfFields = fields.filter(_.cf == cfName)
      val scalaColumns = columns.asScala
      cfFields.map { f =>
        val dataType: SHCDataType = SHCDataTypeFactory.create(f)
        if (f.col.isEmpty && containsDynamic(f.dt)) {
          val m = scalaColumns.map { case (q, versions) =>
            val pq = relation.catalog.shcTableCoder.fromBytes(q).toString
            val v = if (getInternalValueType(f.dt).exists(keepVersions)) {
              versions.asScala.mapValues(dataType.fromBytes)
            } else {
              dataType.fromBytes(versions.firstEntry().getValue)
            }
            pq -> v
          }
          cfFields.foreach(f => m.remove(f.col))
          f -> m.toMap
        } else {
          val pq = relation.catalog.shcTableCoder.toBytes(f.col)
          val timeseries = scalaColumns.get(pq)
          val v = if (keepVersions(f.dt)) {
            timeseries.map(_.asScala.mapValues(dataType.fromBytes))
          } else {
            {
              timeseries.map(ver => dataType.fromBytes(ver.firstEntry().getValue))
            }.getOrElse(null)
          }
          f -> v
        }
      }.toMap
    }

    val unioned = keySeq ++ valuesSeq
    //     Return the row ordered by the requested order
    val ordered = fields.map(unioned.getOrElse(_, null))
    val additional = unioned.filterNot { case (f, _) => fields.contains(f) }.values
    val unstructured = ordered ++ additional
    val finalData = if(isTimeStampColumnRequired) {
      unstructured :+ result.rawCells()(0).getTimestamp
    } else {
      unstructured
    }
    Row.fromSeq(finalData)
  }

  private def keepVersions(dt: DataType): Boolean = {
    dt match {
      case m: MapType => m.keyType.isInstanceOf[LongType.type]
      case _ => false
    }
  }

  private def containsDynamic(dt: DataType): Boolean = {
    dt match {
      case m: MapType => m.keyType.isInstanceOf[StringType.type]
      case _ => false
    }
  }

  private def getInternalValueType(dt: DataType): Option[DataType] = {
    dt match {
      case m: MapType => Some(m.valueType)
      case _ => None
    }
  }
}

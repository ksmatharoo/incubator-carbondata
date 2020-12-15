package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.types.{LongType}
import scala.collection.JavaConverters._

import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{ByteUtil, CarbonUtil}
import org.apache.carbondata.hbase.HBaseConstants

case class CollectHandOffStatsCommand(databaseNameOp: Option[String],
    tableName: String) extends DataCommand {
  override def output: Seq[Attribute] = {
    Seq(
      AttributeReference("maxrowtimestamp", LongType, nullable = true)(),
      AttributeReference("timestampcolumn", LongType, nullable = true)(),
      AttributeReference("noOfRowsInserted", LongType, nullable = true)(),
      AttributeReference("noOfRowsUpdated", LongType, nullable = true)(),
      AttributeReference("noOfRowsDeleted", LongType, nullable = true)()
    )
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv
      .getInstance(sparkSession)
      .carbonMetaStore
      .lookupRelation(databaseNameOp, tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    val carbonTable = relation.carbonTable
    val externalSegmentList = new SegmentStatusManager(carbonTable
      .getAbsoluteTableIdentifier).getValidAndInvalidSegments.getValidSegments.asScala.filter(
      f => !f.getLoadMetadataDetails.isCarbonFormat
    ).asJava

    if(externalSegmentList.isEmpty) {
      return Seq(Row(null, null, 0L, 0L, 0L))
    }
    val externalSchema = CarbonUtil.getExternalSchema(carbonTable.getAbsoluteTableIdentifier)
    val statsColumnName = externalSchema.getParam(HBaseConstants.CARBON_HBASE_STATS_COLUMN)
    val segmentMetaInfoMap = externalSegmentList.get(0).getSegmentMetaDataInfo.getSegmentColumnMetaDataInfoMap
    if (null == segmentMetaInfoMap || segmentMetaInfoMap.isEmpty) {
      return Seq(Row(null, null, 0L, 0L, 0L))
    }
    val timestampColumnValue = ByteUtil.toLong(segmentMetaInfoMap
      .get(statsColumnName)
      .getColumnMinValue, 0, ByteUtil.SIZEOF_LONG)
    val rowTimestampValue = ByteUtil.toLong(segmentMetaInfoMap
      .get(statsColumnName)
      .getColumnMinValue, 0, ByteUtil.SIZEOF_LONG)
    Seq(Row(timestampColumnValue,
      rowTimestampValue,
      0L,0L,0L))
  }

  override protected def opName: String = {
    "Collect HandOff Stats"
  }
}

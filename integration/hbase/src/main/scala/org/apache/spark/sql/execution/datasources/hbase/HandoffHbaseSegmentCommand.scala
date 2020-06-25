package org.apache.spark.sql.execution.datasources.hbase

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericRow}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoWithDf
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.transaction.{TransactionActionType, TransactionManager}
import org.apache.carbondata.core.util.{ByteUtil, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.tranaction.SessionTransactionManager

case class HandoffHbaseSegmentCommand(
    databaseNameOp: Option[String],
    tableName: String,
    graceTimeinMillis: Long,
    deleteRows: Boolean = true)
  extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val rltn = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(databaseNameOp, tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    val carbonTable = rltn.carbonTable
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    // if insert overwrite in progress, do not allow add segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "handoff segment")
    }

    val details =
      SegmentStatusManager.readLoadMetadata(
        CarbonTablePath.getMetadataPath(carbonTable.getTablePath))
    val detail = details.filter(_.getSegmentStatus.equals(SegmentStatus.SUCCESS)).
      find(_.getFileFormat.getFormat.equalsIgnoreCase("hbase"))
      .getOrElse(throw new AnalysisException(s"Segment with format hbase doesn't exist"))
    val transactionManager = TransactionManager
      .getInstance()
      .getTransactionManager
      .asInstanceOf[SessionTransactionManager]
    val fullTableName = carbonTable.getDatabaseName + "." + carbonTable.getTableName
    var transactionId = transactionManager.getTransactionId(sparkSession,
      fullTableName)
    var isTransctionStarted = false
    if (transactionId == null) {
      isTransctionStarted = true
      transactionId = transactionManager.startTransaction(sparkSession)
    }

    val externalSchema = CarbonUtil.getExternalSchemaString(carbonTable.getAbsoluteTableIdentifier)

    val tableCols =
      carbonTable.getTableInfo
        .getFactTable
        .getListOfColumns
        .asScala
        .sortBy(_.getSchemaOrdinal)
        .map(_.getColumnName)
        .
          filterNot(_.equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
    val header = tableCols.mkString(",")
    val store = new SegmentFileStore(carbonTable.getTablePath, detail.getSegmentFile)
    val columnMetaDataInfo = store.getSegmentFile
      .getSegmentMetaDataInfo
      .getSegmentColumnMetaDataInfoMap
      .get("rowtimestamp")
    val minTimestamp = if (columnMetaDataInfo != null) {
      ByteUtil.toLong(columnMetaDataInfo.getColumnMinValue, 0, ByteUtil.SIZEOF_LONG) + 1
    } else {
      0L
    }
    val hBaseRelation = new CarbonHBaseRelation(Map(
      HBaseTableCatalog.tableCatalog -> externalSchema,
      HBaseRelation.MIN_STAMP -> minTimestamp.toString,
      HBaseRelation.MAX_STAMP -> Long.MaxValue.toString), Option.empty)(sparkSession.sqlContext)
    val rdd = hBaseRelation.buildScan(hBaseRelation.schema.map(f => f.name).toArray, Array.empty)
      .map(f => new GenericInternalRow(f.asInstanceOf[GenericRow].values).asInstanceOf[InternalRow])
    val loadDF = Dataset.ofRows(sparkSession,
      LogicalRDD(hBaseRelation.schema.toAttributes, rdd)(sparkSession)).cache()
    val tempView = UUID.randomUUID().toString.replace("-", "")
    loadDF.createOrReplaceTempView(tempView)
    val rows = sparkSession.sql(s"select max(rowtimestamp) from $tempView")
      .collect()
    if (rows.isEmpty) {
      transactionManager.rollbackTransaction(transactionId);
      return Seq.empty
    }
    val maxTimeStamp = rows.head.getLong(0) - graceTimeinMillis
    val updated = loadDF.where(col("rowtimestamp").leq(lit(maxTimeStamp)))

    CarbonInsertIntoWithDf(
      databaseNameOp = Some(carbonTable.getDatabaseName),
      tableName = carbonTable.getTableName,
      options = Map(("fileheader" -> header)),
      isOverwriteTable = false,
      dataFrame = updated.select(tableCols.map(col): _*),
      updateModel = None,
      tableInfoOp = Some(carbonTable.getTableInfo)).process(sparkSession)

    val segmentId = transactionManager
      .getAndSetCurrentTransactionSegment(transactionId,
        carbonTable.getDatabaseName + "." + carbonTable.getTableName)
    transactionManager.recordTransactionAction(transactionId,
      new HbaseSegmentTransactionAction(carbonTable, segmentId, detail.getLoadName, maxTimeStamp),
      TransactionActionType.COMMIT_SCOPE)

    if (isTransctionStarted) {
      transactionManager.commitTransaction(transactionId)
    }

    // delete rows
    if(deleteRows) {
      val hBaseRelationForDelete =
        new CarbonHBaseRelation(Map(HBaseTableCatalog.tableCatalog -> externalSchema,
          "deleterows" -> "true"), Option.empty)(sparkSession.sqlContext)
      hBaseRelationForDelete.insert(updated.select(hBaseRelationForDelete.schema
        .map(_.name)
        .map(col): _*), false)
    }

    Seq.empty
  }


  override protected def opName: String = {
    "ALTER SEGMENT ON TABLE tableName HANDOFF STREAMING SEGMENT "
  }
}
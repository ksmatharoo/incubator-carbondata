package org.apache.spark.sql.execution.datasources.hbase

import java.io.File
import java.util
import java.util.Arrays

import org.apache.hadoop.fs.FileStatus
import scala.collection.JavaConverters._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.segmentmeta.{SegmentColumnMetaDataInfo, SegmentMetaDataInfo}
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus,
  SegmentStatusManager}
import org.apache.carbondata.core.transaction.TransactionAction
import org.apache.carbondata.core.util.ByteUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

class HbaseSegmentTransactionAction(carbonTable: CarbonTable,
    loadingSegment: String,
    prevSegment: String,
    maxTimeStamp: Long) extends TransactionAction {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def commit(): Unit = {


    val model = new CarbonLoadModel
    model.setCarbonTransactionalTable(true)
    model.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
    model.setDatabaseName(carbonTable.getDatabaseName)
    model.setTableName(carbonTable.getTableName)
    val detailses = SegmentStatusManager.readLoadMetadata(CarbonTablePath
      .getMetadataPath(
        carbonTable.getTablePath))
    val hbaseDetail = detailses.find(_.getLoadName.equals(loadingSegment))
    if (hbaseDetail.isEmpty) {
      throw new MalformedCarbonCommandException(s"Segmentid $loadingSegment is not found")
    }
    // TODO Update min values for incremental columns in SegmentMetaDataInfo
    //    val fileStore = SegmentFileStore.readSegmentFile(CarbonTablePath.getSegmentFilePath
    //    (carbonTable
    //      .getTablePath, hbaseDetail.get.getSegmentFile))

    model.setLoadMetadataDetails(detailses.toList.asJava)
    val newLoadMetaEntry = new LoadMetadataDetails
    model.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime)
    CarbonLoaderUtil.populateNewLoadMetaEntry(newLoadMetaEntry,
      SegmentStatus.SUCCESS,
      model.getFactTimeStamp,
      false)
    newLoadMetaEntry.setFileFormat(new FileFormat("hbase"))
    CarbonLoaderUtil.recordNewLoadMetadata(newLoadMetaEntry, model, true, false)
    val columnMinMaxInfo = new util.HashMap[String, SegmentColumnMetaDataInfo]()
    carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala.
      map(f => (f.getColumnName, new SegmentColumnMetaDataInfo(false,
        Array[Byte](0),
        Array[Byte](0),
        false))).foreach(f => columnMinMaxInfo.put(f._1, f._2))
    val segmentMetaDataInto = new SegmentMetaDataInfo(columnMinMaxInfo)
    columnMinMaxInfo.put("rowtimestamp",
      new SegmentColumnMetaDataInfo(false, ByteUtil.toBytes(maxTimeStamp), Array[Byte](0), false))
    val segment = new Segment(
      model.getSegmentId,
      SegmentFileStore.genSegmentFileName(
        model.getSegmentId,
        System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
      "",
      new util.HashMap[String, String]())

    val writeSegment =
      SegmentFileStore.writeSegmentFileForExternalStreaming(
        carbonTable,
        segment,
        null,
        new util.ArrayList[FileStatus](),
        segmentMetaDataInto,
        false)

    val success = if (writeSegment) {
      newLoadMetaEntry.setSegmentFile(segment.getSegmentFileName)

      CarbonLoaderUtil.writeTableStatus(model,
        newLoadMetaEntry,
        false,
        false,
        util.Arrays.asList(Segment.toSegment(prevSegment)),
        "",
        model.getFactTimeStamp)
    } else {
      false
    }

    if (!success) {
      CarbonLoaderUtil.updateTableStatusForFailure(model, "uniqueTableStatusId")
      LOGGER.info("********starting clean up**********")
      // delete segment is applicable for transactional table
      CarbonLoaderUtil.deleteSegment(model, model.getSegmentId.toInt)
      // delete corresponding segment file from metadata
      val segmentFile = CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath) +
                        File.separator + segment.getSegmentFileName
      FileFactory.deleteFile(segmentFile)
      LOGGER.info("********clean up done**********")
      LOGGER.error("Data load failed due to failure in table status updation.")
      throw new Exception("Data load failed due to failure in table status updation.")
    }
  }

}

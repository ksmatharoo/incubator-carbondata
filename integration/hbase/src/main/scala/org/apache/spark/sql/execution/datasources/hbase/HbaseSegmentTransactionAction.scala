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

import java.io.File
import java.util

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
      SegmentStatus.INSERT_IN_PROGRESS,
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
    val hbaseCurrentSuccessSegment = detailses.filter(det => det.getFileFormat.toString.equals("hbase") &&
                                             det.getSegmentStatus == SegmentStatus.SUCCESS).head
    hbaseCurrentSuccessSegment.setSegmentStatus(SegmentStatus.COMPACTED)
    hbaseCurrentSuccessSegment.setModificationOrdeletionTimesStamp(model.getFactTimeStamp)
    hbaseCurrentSuccessSegment.setMergedLoadName(newLoadMetaEntry.getLoadName)

    val success = if (writeSegment) {
      newLoadMetaEntry.setSegmentFile(segment.getSegmentFileName)
      newLoadMetaEntry.setDataSize("0")
      newLoadMetaEntry.setIndexSize("0")
      newLoadMetaEntry.setSegmentStatus(SegmentStatus.SUCCESS)
      val allLoadMetadata = detailses :+ newLoadMetaEntry
      SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(
        carbonTable.getAbsoluteTableIdentifier.getTablePath),
        allLoadMetadata)
      true
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

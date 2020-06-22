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

package org.apache.spark.sql.execution.command.management

import java.io.{DataOutputStream, File}
import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.execution.command.{Checker, MetadataCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.segmentmeta.{SegmentColumnMetaDataInfo, SegmentMetaDataInfo}
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus,
  SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

case class CarbonAddExternalStreamingSegmentCommand(dbName: Option[String],
    tableName: String,
    options: Map[String, String]) extends MetadataCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override protected def opName: String = "Add Streaming Segment Command"

  val supportedFormats: Seq[String] =
    Seq("HBase")

  private def validateFormat(format: String): Boolean = {
    supportedFormats.exists(_.equalsIgnoreCase(format))
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(dbName, tableName, sparkSession)
    val relation = CarbonEnv
      .getInstance(sparkSession)
      .carbonMetaStore
      .lookupRelation(dbName, tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    val carbonTable = relation.carbonTable
    setAuditTable(carbonTable)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (carbonTable.hasMVCreated) {
      throw new MalformedCarbonCommandException("Unsupported operation on MV table")
    }
    // if insert overwrite in progress, do not allow add segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "delete segment")
    }

    if (!validateFormat(options.getOrElse("format", "HBase"))) {
      throw new MalformedCarbonCommandException(
        "Invalid format: " + options.getOrElse("format", "HBase") + " Valid Formats are:" +
        supportedFormats)
    }
    val schema = options.getOrElse("segmentSchema", "")
    if (schema.isEmpty) {
      throw new MalformedCarbonCommandException("Streaming segment schema cannot be empty")
    }
    writeMetaForSegment(sparkSession, carbonTable)
    writeExternalSchema(carbonTable, schema);
    Seq.empty
  }

  private def writeExternalSchema(carbonTable: CarbonTable, schema: String): Unit = {
    val metadataPath = CarbonTablePath.getMetadataPath(carbonTable.getTablePath)
    var stream: DataOutputStream = null
    try {
      stream = FileFactory.getDataOutputStream(metadataPath + "/" + "externalSchema")
      stream.write(schema.getBytes(CarbonCommonConstants.DEFAULT_CHARSET))
    }
    catch {
      case e: Exception => throw e
    } finally {
      CarbonUtil.closeStream(stream)
    }
  }

  /**
   * Write metadata for external segment, including table status file and segment file
   *
   * @param sparkSession spark session
   * @param carbonTable  carbon table
   */
  private def writeMetaForSegment(
      sparkSession: SparkSession,
      carbonTable: CarbonTable
  ): Unit = {
    val model = new CarbonLoadModel
    model.setCarbonTransactionalTable(true)
    model.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
    model.setDatabaseName(carbonTable.getDatabaseName)
    model.setTableName(carbonTable.getTableName)

    val newLoadMetaEntry = new LoadMetadataDetails
    model.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime)
    CarbonLoaderUtil.populateNewLoadMetaEntry(newLoadMetaEntry,
      SegmentStatus.INSERT_IN_PROGRESS,
      model.getFactTimeStamp,
      false)
    val format = options.getOrElse("format", "HBase")
    newLoadMetaEntry.setFileFormat(new FileFormat(format))

    CarbonLoaderUtil.recordNewLoadMetadata(newLoadMetaEntry, model, true, false)
    val segmentMetaDataInto = new SegmentMetaDataInfo(new util.HashMap[String,
      SegmentColumnMetaDataInfo]())
    val updatedOptions = options - "segmentSchema"
    val segment = new Segment(
      model.getSegmentId,
      SegmentFileStore.genSegmentFileName(
        model.getSegmentId,
        System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
      "",
      new util.HashMap[String, String](updatedOptions.asJava))

    val writeSegment =
      SegmentFileStore.writeSegmentFileForExternalStreaming(
        carbonTable,
        segment,
        null,
        new util.ArrayList[FileStatus](),
        segmentMetaDataInto,
        !format.equalsIgnoreCase("hbase"))

    val success = if (writeSegment) {
      SegmentFileStore.updateTableStatusFile(
        carbonTable,
        model.getSegmentId,
        segment.getSegmentFileName,
        carbonTable.getCarbonTableIdentifier.getTableId,
        new SegmentFileStore(carbonTable.getTablePath, segment.getSegmentFileName),
        SegmentStatus.SUCCESS)
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

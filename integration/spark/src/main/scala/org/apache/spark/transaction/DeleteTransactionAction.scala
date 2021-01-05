package org.apache.spark.transaction

import java.util
import java.util.Objects

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.sql.execution.command.mutation.DeleteExecution.LOGGER

import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, SegmentUpdateDetails}
import org.apache.carbondata.core.mutate.data.BlockMappingVO
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.transaction.TransactionAction
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.tranaction.DeleteActionMetadata


class DeleteTransactionAction(databaseNameOp: Option[String],
    tableName: String,
    sparkSession: SparkSession,
    executorErrors: ExecutionErrors,
    res: Array[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]],
    carbonTable: CarbonTable,
    timestamp: String,
    blockMappingVO: BlockMappingVO,
    isUpdateOperation: Boolean,
    deleteActionMetadata:DeleteActionMetadata
  ) extends TransactionAction {

  var metadataDetails:Array[LoadMetadataDetails]  = _

  override def commit(): Unit = {
    metadataDetails = SegmentStatusManager.readTableStatusFile(
      CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath))
    val (deletedSegments, deletedRowCount) = updateTableStatusFile()
    deleteActionMetadata.setDeletedRowCount(deletedRowCount)
    deleteActionMetadata.setSegmentsToBeDeleted(deletedSegments.asJava)
  }

  def updateTableStatusFile(): (Seq[Segment], Long)  = {
    var segmentsTobeDeleted = Seq.empty[Segment]
    var operatedRowCount = 0L
    // if no loads are present then no need to do anything.
    if (res.flatten.isEmpty) {
      return (segmentsTobeDeleted, operatedRowCount)
    }
    // update new status file
    segmentsTobeDeleted = checkAndUpdateStatusFiles()

    if (executorErrors.failureCauses == FailureCauses.NONE) {
      operatedRowCount = res.flatten.map(_._2._3).sum
    }
    (segmentsTobeDeleted, operatedRowCount)
  }

  // all or none : update status file, only if complete delete opeartion is successfull.
  def checkAndUpdateStatusFiles(): Seq[Segment] = {
    val blockUpdateDetailsList = new util.ArrayList[SegmentUpdateDetails]()
    val segmentDetails = new util.HashSet[Segment]()
    res.foreach(resultOfSeg => resultOfSeg.foreach(
      resultOfBlock => {
        if (resultOfBlock._1 == SegmentStatus.SUCCESS) {
          blockUpdateDetailsList.add(resultOfBlock._2._1)
          segmentDetails.add(new Segment(resultOfBlock._2._1.getSegmentName))
          // if this block is invalid then decrement block count in map.
          if (CarbonUpdateUtil.isBlockInvalid(resultOfBlock._2._1.getSegmentStatus)) {
            CarbonUpdateUtil.decrementDeletedBlockCount(resultOfBlock._2._1,
              blockMappingVO.getSegmentNumberOfBlockMapping)
          }
        } else {
          // In case of failure , clean all related delete delta files
          CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)
          val errorMsg =
            "Delete data operation is failed due to failure in creating delete delta file for " +
            "segment : " + resultOfBlock._2._1.getSegmentName + " block : " +
            resultOfBlock._2._1.getBlockName
          executorErrors.failureCauses = resultOfBlock._2._2.failureCauses
          executorErrors.errorMsg = resultOfBlock._2._2.errorMsg

          if (executorErrors.failureCauses == FailureCauses.NONE) {
            executorErrors.failureCauses = FailureCauses.EXECUTOR_FAILURE
            executorErrors.errorMsg = errorMsg
          }
          LOGGER.error(errorMsg)
          return Seq.empty[Segment]
        }
      }))

    val listOfSegmentToBeMarkedDeleted = CarbonUpdateUtil
      .getListOfSegmentsToMarkDeleted(blockMappingVO.getSegmentNumberOfBlockMapping)

    val segmentsTobeDeleted = listOfSegmentToBeMarkedDeleted.asScala

    // this is delete flow so no need of putting timestamp in the status file.
    if (CarbonUpdateUtil
          .updateSegmentStatus(blockUpdateDetailsList, carbonTable, timestamp, false) &&
        CarbonUpdateUtil
          .updateTableMetadataStatus(segmentDetails,
            carbonTable,
            timestamp,
            !isUpdateOperation,
            listOfSegmentToBeMarkedDeleted)
    ) {
      LOGGER.info(s"Delete data operation is successful for " +
                  s"${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
    } else {
      // In case of failure , clean all related delete delta files
      CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)
      val errorMessage = "Delete data operation is failed due to failure " +
                         "in table status updation."
      LOGGER.error("Delete data operation is failed due to failure in table status updation.")
      executorErrors.failureCauses = FailureCauses.STATUS_FILE_UPDATION_FAILURE
      executorErrors.errorMsg = errorMessage
    }
    throw new RuntimeException("failed")
//    segmentsTobeDeleted
  }


  override def rollback(): Unit = {
    if (Objects.nonNull(metadataDetails) && metadataDetails.length > 0) {
      SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(
        carbonTable.getTablePath), metadataDetails)
    }
  }


}

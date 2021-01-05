package org.apache.spark.transaction

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.mutation.{DeleteExecution, HorizontalCompaction, HorizontalCompactionException}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.transaction.TransactionAction
import org.apache.carbondata.events.{DeleteFromTablePostEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.tranaction.DeleteActionMetadata

class DeletePostTransactionAction(sparkSession: SparkSession,
    carbonTable: CarbonTable,
    deleteActionMetadata: DeleteActionMetadata,
    operationContext: OperationContext,
    timestamp: String,
    metadataLock: ICarbonLock,
    updateLock: ICarbonLock,
    compactionLock: ICarbonLock,
    lockStatus:Boolean) extends TransactionAction {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def commit(): Unit = {
    try {
      // call IUD Compaction.
      HorizontalCompaction.tryHorizontalCompaction(sparkSession, carbonTable,
        isUpdateOperation = false)

      val allDataMapSchemas = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
        .filter(dataMapSchema => null != dataMapSchema.getRelationIdentifier &&
                                 !dataMapSchema.isIndexDataMap).asJava
      if (!allDataMapSchemas.isEmpty) {
        DataMapStatusManager.truncateDataMap(allDataMapSchemas)
      }

      // prepriming for delete command
      DeleteExecution.reloadDistributedSegmentCache(carbonTable,
        deleteActionMetadata.getSegmentsToBeDeleted.asScala, operationContext)(sparkSession)

      // trigger post event for Delete from table
      val deleteFromTablePostEvent: DeleteFromTablePostEvent =
        DeleteFromTablePostEvent(sparkSession, carbonTable)
      OperationListenerBus.getInstance.fireEvent(deleteFromTablePostEvent, operationContext)
      Seq(Row(deleteActionMetadata.getDeletedRowCount))
    } catch {
      case e: HorizontalCompactionException =>
        LOGGER.error("Delete operation passed. Exception in Horizontal Compaction." +
                     " Please check logs. " + e.getMessage)
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, e.compactionTimeStamp.toString)
        Seq(Row(0L))

      case e: Exception =>
        LOGGER.error("Exception in Delete data operation " + e.getMessage, e)
        // ****** start clean up.
        // In case of failure , clean all related delete delta files
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)

        // clean up. Null check is required as for executor error some times message is null
        if (null != e.getMessage) {
          sys.error("Delete data operation is failed. " + e.getMessage)
        }
        else {
          sys.error("Delete data operation is failed. Please check logs.")
        }
    } finally {
      if (lockStatus) {
        CarbonLockUtil.fileUnlock(metadataLock, LockUsage.METADATA_LOCK)
      }
      updateLock.unlock()
      compactionLock.unlock()
    }
  }
}

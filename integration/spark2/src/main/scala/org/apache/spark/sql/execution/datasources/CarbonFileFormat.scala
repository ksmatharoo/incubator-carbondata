package org.apache.spark.sql.execution.datasources

import java.io.File

import scala.collection.JavaConverters._

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.api.{CarbonOutputCommitter, CarbonTableOutputFormat}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.util.{DataLoadingUtil, Util}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import scala.util.Random

import org.apache.carbondata.core.metadata.schema.table.CarbonTable

class CarbonFileFormat
  extends FileFormat
    with DataSourceRegister
    with Logging
with Serializable {

  override def shortName() = "carbondata"

  override def inferSchema(sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]) = {
    None
  }

  override def prepareWrite(sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType) = {
    val conf = job.getConfiguration
    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      classOf[CarbonOutputCommitter],
      classOf[CarbonOutputCommitter])

    job.setOutputFormatClass(classOf[CarbonTableOutputFormat])

    var table = CarbonMetadata.getInstance().getCarbonTable(
      options.getOrElse("dbName", "default"), options.get("tableName").get)
//    table = CarbonTable.buildFromTableInfo(table.getTableInfo, true)
    val model = new CarbonLoadModel
    val carbonProperty = CarbonProperties.getInstance()
    val optionsFinal = DataLoadingUtil.getDataLoadingOptions(carbonProperty, options)
    val tableProperties = table.getTableInfo.getFactTable.getTableProperties
    optionsFinal.put("sort_scope", tableProperties.asScala.getOrElse("sort_scope",
      carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
        carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
          CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))
    val partitionStr = table.getTableInfo.getFactTable.getPartitionInfo.getColumnSchemaList.asScala.map(_.getColumnName.toLowerCase).mkString(",")
    optionsFinal.put("fileheader", dataSchema.fields.map(_.name.toLowerCase).mkString(",") + "," + partitionStr)
    DataLoadingUtil.buildCarbonLoadModel(
      table,
      carbonProperty,
      options,
      optionsFinal,
      model,
      conf
    )
    model.setPartitionId("0")
    CarbonTableOutputFormat.setLoadModel(conf, model)
    CarbonTableOutputFormat.setOverwrite(conf, options("overwrite").toBoolean)

    new OutputWriterFactory {

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        val partitions = getPartitionsFromPath(path, context)
        val isCarbonUseMultiDir = CarbonProperties.getInstance().isUseMultiTempDir
        var storeLocation: Array[String] = Array[String]()
        val isCarbonUseLocalDir = CarbonProperties.getInstance()
          .getProperty("carbon.use.local.dir", "false").equalsIgnoreCase("true")
        val tmpLocationSuffix = File.separator + System.nanoTime()
        if (isCarbonUseLocalDir) {
          val yarnStoreLocations = Util.getConfiguredLocalDirs(SparkEnv.get.conf)
          if (!isCarbonUseMultiDir && null != yarnStoreLocations && yarnStoreLocations.nonEmpty) {
            // use single dir
            storeLocation = storeLocation :+
              (yarnStoreLocations(Random.nextInt(yarnStoreLocations.length)) + tmpLocationSuffix)
            if (storeLocation == null || storeLocation.isEmpty) {
              storeLocation = storeLocation :+
                (System.getProperty("java.io.tmpdir") + tmpLocationSuffix)
            }
          } else {
            // use all the yarn dirs
            storeLocation = yarnStoreLocations.map(_ + tmpLocationSuffix)
          }
        } else {
          storeLocation = storeLocation :+ (System.getProperty("java.io.tmpdir") + tmpLocationSuffix)
        }
        CarbonTableOutputFormat.setTempStoreLocations(context.getConfiguration, storeLocation)
        new CarbonOutputWriter(path, context, dataSchema.map(_.dataType), partitions.map(_.split("=")(1)))
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".carbondata"
      }

      def getPartitionsFromPath(path: String, attemptContext: TaskAttemptContext): Array[String] = {
        val attemptId = attemptContext.getTaskAttemptID.toString + "/"
        val str = path.substring(path.indexOf(attemptId) + attemptId.length, path.lastIndexOf("/"))
        str.split("/")
      }
    }
  }

}

private class CarbonOutputWriter(path: String,
    context: TaskAttemptContext,
    fieldTypes: Seq[DataType],
    partitionData: Array[String])
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, Array[String]] = {
    new CarbonTableOutputFormat() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }.getRecordWriter(context)
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override def writeInternal(row: InternalRow): Unit = {
    val data = new Array[String](fieldTypes.length + partitionData.length)
    var i = 0
    while (i < fieldTypes.length) {
      data(i) = row.get(i, fieldTypes(i)).toString
      i += 1
    }
    System.arraycopy(partitionData, 0, data, fieldTypes.length, partitionData.length)
    recordWriter.write(null, data)
  }

  override def close(): Unit = recordWriter.close(context)
}

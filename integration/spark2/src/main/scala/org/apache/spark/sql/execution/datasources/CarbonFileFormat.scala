package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.carbondata.hadoop.api.{CarbonOutputCommitter, CarbonTableOutputFormat}

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
    val conf = ContextUtil.getConfiguration(job)
    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      classOf[CarbonOutputCommitter],
      classOf[CarbonOutputCommitter])

    job.setOutputFormatClass(classOf[CarbonTableOutputFormat])

    new OutputWriterFactory {

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new CarbonOutputWriter(path, context, dataSchema)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".carbondata"
      }
    }
  }

}

private class CarbonOutputWriter(path: String, context: TaskAttemptContext, schema: StructType)
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, Array[String]] = {
    new CarbonTableOutputFormat() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }.getRecordWriter(context)
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override def writeInternal(row: InternalRow): Unit =
    recordWriter.write(null, row.toSeq(schema).asInstanceOf[Array[String]])

  override def close(): Unit = recordWriter.close(context)
}

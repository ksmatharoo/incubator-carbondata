package org.apache.carbondata.hadoop.api;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class CarbonOutputCommitter extends FileOutputCommitter {

  public CarbonOutputCommitter(Path outputPath, JobContext context) throws IOException {
    super(outputPath, context);
  }

  @Override public void commitJob(JobContext context) throws IOException {
    super.commitJob(context);
    // TODO we can do any job commit activities here like table status update or writing of
    // summary file here.
  }
}

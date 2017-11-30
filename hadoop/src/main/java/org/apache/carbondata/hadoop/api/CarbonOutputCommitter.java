package org.apache.carbondata.hadoop.api;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

public class CarbonOutputCommitter extends FileOutputCommitter {

  public CarbonOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
  }

  @Override public void setupJob(JobContext context) throws IOException {
    super.setupJob(context);
    boolean overwriteSet = CarbonTableOutputFormat.isOverwriteSet(context.getConfiguration());
    CarbonLoadModel loadModel = CarbonTableOutputFormat.getLoadModel(context.getConfiguration());
    try {
      CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, overwriteSet);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    CarbonTableOutputFormat.setLoadModel(context.getConfiguration(), loadModel);
  }

  @Override public void commitJob(JobContext context) throws IOException {
    super.commitJob(context);
    boolean overwriteSet = CarbonTableOutputFormat.isOverwriteSet(context.getConfiguration());
    CarbonLoadModel loadModel = CarbonTableOutputFormat.getLoadModel(context.getConfiguration());
    try {
      LoadMetadataDetails newMetaEntry =
          loadModel.getLoadMetadataDetails().get(loadModel.getLoadMetadataDetails().size() - 1);
      CarbonLoaderUtil.populateNewLoadMetaEntry(newMetaEntry, SegmentStatus.SUCCESS,
          loadModel.getFactTimeStamp(), true);
      CarbonUtil.addDataIndexSizeIntoMetaEntry(newMetaEntry, loadModel.getSegmentId(),
          loadModel.getCarbonDataLoadSchema().getCarbonTable());
      CarbonLoaderUtil.recordLoadMetadata(newMetaEntry, loadModel, false, overwriteSet);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override public void abortJob(JobContext context, JobStatus.State state) throws IOException {
    super.abortJob(context, state);
    CarbonLoadModel loadModel = CarbonTableOutputFormat.getLoadModel(context.getConfiguration());
    try {
      CarbonLoaderUtil.updateTableStatusForFailure(loadModel);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}

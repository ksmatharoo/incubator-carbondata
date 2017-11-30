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

package org.apache.carbondata.hadoop.api;

import java.io.IOException;
import java.util.Random;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.iterator.CarbonOutputIteratorWrapper;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Base class for all output format for CarbonData file.
 */
public class CarbonTableOutputFormat extends FileOutputFormat<Void, String[]> {

  private static String LOAD_MODEL = "carbon.load.model";
  private static String TEMP_STORE_LOCATIONS = "carbon.load.tempstore.locations";
  private static String OVERWRITE_SET = "carbon.load.set.overwrite";

  private CarbonOutputCommitter committer;

  public static void setLoadModel(Configuration configuration, CarbonLoadModel loadModel)
      throws IOException {
    if (loadModel != null) {
      configuration.set(LOAD_MODEL, ObjectSerializationUtil.convertObjectToString(loadModel));
    }
  }

  public static CarbonLoadModel getLoadModel(Configuration configuration) throws IOException {
    String encodedString = configuration.get(LOAD_MODEL);
    if (encodedString != null) {
      return (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(encodedString);
    }
    return null;
  }

  public static void setTempStoreLocations(Configuration configuration, String[] tempLocations)
      throws IOException {
    if (tempLocations != null && tempLocations.length > 0) {
      configuration
          .set(TEMP_STORE_LOCATIONS, ObjectSerializationUtil.convertObjectToString(tempLocations));
    }
  }

  public static boolean isOverwriteSet(Configuration configuration) {
    String overwrite = configuration.get(OVERWRITE_SET);
    if (overwrite != null) {
      return Boolean.parseBoolean(overwrite);
    }
    return false;
  }

  public static void setOverwrite(Configuration configuration, boolean overwrite) {
    configuration.set(OVERWRITE_SET, String.valueOf(overwrite));
  }

  private static String[] getTempStoreLocations(Configuration configuration) throws IOException {
    String encodedString = configuration.get(TEMP_STORE_LOCATIONS);
    if (encodedString != null) {
      return (String[]) ObjectSerializationUtil.convertStringToObject(encodedString);
    }
    return null;
  }

  @Override public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (this.committer == null) {
      Path output = getOutputPath(context);
      this.committer = new CarbonOutputCommitter(output, context);
    }

    return this.committer;
  }

  @Override
  public RecordWriter<Void, String[]> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    final CarbonLoadModel loadModel = getLoadModel(taskAttemptContext.getConfiguration());
    loadModel.setTaskNo(new Random().nextInt(Integer.MAX_VALUE) + "");
    final String[] tempStoreLocations = getTempStoreLocations(taskAttemptContext.getConfiguration());
    final CarbonOutputIteratorWrapper iteratorWrapper = new CarbonOutputIteratorWrapper();
    CarbonRecordWriter recordWriter = new CarbonRecordWriter(iteratorWrapper);
    new Thread() {
      @Override public void run() {
        try {
          new DataLoadExecutor()
              .execute(loadModel, tempStoreLocations, new CarbonIterator[] { iteratorWrapper });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }.start();

    return recordWriter;
  }

  private static class CarbonRecordWriter extends RecordWriter<Void, String[]> {

    private CarbonOutputIteratorWrapper iteratorWrapper;

    public CarbonRecordWriter(CarbonOutputIteratorWrapper iteratorWrapper) {
      this.iteratorWrapper = iteratorWrapper;
    }

    @Override public void write(Void aVoid, String[] strings)
        throws IOException, InterruptedException {
      iteratorWrapper.write(strings);
    }

    @Override public void close(TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
      iteratorWrapper.close();
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.processing.sortandgroupby.sortdata;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

public class SortIntermediateFileMerger {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortIntermediateFileMerger.class.getName());

  /**
   * executorService
   */
  private ExecutorService executorService;
  /**
   * procFiles
   */
  private List<File> procFiles;

  private SortParameters parameters;

  private final Object lockObject = new Object();

  public SortIntermediateFileMerger(SortParameters parameters) {
    this.parameters = parameters;
    // processed file list
    this.procFiles = new ArrayList<File>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    this.executorService = Executors.newFixedThreadPool(parameters.getNumberOfCores());
  }

  public void addFileToMerge(File sortTempFile) {
    // add sort temp filename to and arrayList. When the list size reaches 20 then
    // intermediate merging of sort temp files will be triggered
    synchronized (lockObject) {
      procFiles.add(sortTempFile);
    }
  }

  public void startMergingIfPossible() {
    File[] fileList;
    if (procFiles.size() >= parameters.getNumberOfIntermediateFileToBeMerged()) {
      synchronized (lockObject) {
        fileList = procFiles.toArray(new File[procFiles.size()]);
        this.procFiles = new ArrayList<File>();
      }
      LOGGER.debug("Sumitting request for intermediate merging no of files: " + fileList.length);
      startIntermediateMerging(fileList);
    }
  }

  /**
   * Below method will be used to start the intermediate file merging
   *
   * @param intermediateFiles
   */
  private void startIntermediateMerging(File[] intermediateFiles) {
    File file = new File(
        parameters.getTempFileLocation() + File.separator + parameters.getTableName() + System
            .nanoTime() + CarbonCommonConstants.MERGERD_EXTENSION);
    IntermediateFileMerger merger = new IntermediateFileMerger(parameters, intermediateFiles, file);
    executorService.submit(merger);
  }

  public void finish() throws CarbonSortKeyAndGroupByException {
    try {
    executorService.shutdown();
    executorService.awaitTermination(2, TimeUnit.DAYS);
  } catch (InterruptedException e) {
    throw new CarbonSortKeyAndGroupByException("Problem while shutdown the server ", e);
  }
    procFiles.clear();
    procFiles = null;
  }

  public void close() {
    if(executorService.isShutdown()) {
      executorService.shutdownNow();
    }
  }

}

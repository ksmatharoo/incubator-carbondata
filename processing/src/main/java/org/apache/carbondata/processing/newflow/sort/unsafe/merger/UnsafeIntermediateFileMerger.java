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

package org.apache.carbondata.processing.newflow.sort.unsafe.merger;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.newflow.sort.unsafe.CarbonUnsafe;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.SortTempChunkHolder;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeSortTempFileChunkHolder;
import org.apache.carbondata.processing.newflow.sort.unsafe.memory.MemoryAllocator;
import org.apache.carbondata.processing.newflow.sort.unsafe.memory.MemoryBlock;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.sortandgroupby.sortdata.TempSortFileWriter;
import org.apache.carbondata.processing.sortandgroupby.sortdata.TempSortFileWriterFactory;

public class UnsafeIntermediateFileMerger implements Callable<Void> {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeIntermediateFileMerger.class.getName());

  /**
   * recordHolderHeap
   */
  private AbstractQueue<SortTempChunkHolder> recordHolderHeap;

  /**
   * fileCounter
   */
  private int fileCounter;

  /**
   * stream
   */
  private DataOutputStream stream;

  /**
   * totalNumberOfRecords
   */
  private int totalNumberOfRecords;

  /**
   * records
   */
  private Object[][] records;

  /**
   * entryCount
   */
  private int entryCount;

  /**
   * writer
   */
  private TempSortFileWriter writer;

  /**
   * totalSize
   */
  private int totalSize;

  private SortParameters mergerParameters;

  private File[] intermediateFiles;

  private File outPutFile;

  private boolean useKettle;

  private boolean[] noDictionarycolumnMapping;

  private long[] nullSetWords;

  private MemoryBlock rowData;

  /**
   * IntermediateFileMerger Constructor
   */
  public UnsafeIntermediateFileMerger(SortParameters mergerParameters, File[] intermediateFiles,
      File outPutFile) {
    this.mergerParameters = mergerParameters;
    this.fileCounter = intermediateFiles.length;
    this.intermediateFiles = intermediateFiles;
    this.outPutFile = outPutFile;
    this.useKettle = mergerParameters.isUseKettle();
    noDictionarycolumnMapping = mergerParameters.getNoDictionaryDimnesionColumn();
    this.nullSetWords = new long[((mergerParameters.getMeasureColCount() - 1) >> 6) + 1];
    // Take size of 2 MB for each row. I think it is high enough to use
    rowData = MemoryAllocator.HEAP.allocate(2*1024*1024);
  }

  @Override public Void call() throws Exception {
    long intermediateMergeStartTime = System.currentTimeMillis();
    int fileConterConst = fileCounter;
    boolean isFailed = false;
    try {
      startSorting();
      initialize();
      while (hasNext()) {
        writeDataTofile(next());
      }
      if (mergerParameters.isSortFileCompressionEnabled() || mergerParameters.isPrefetch()) {
        if (entryCount > 0) {
          if (entryCount < totalSize) {
            Object[][] temp = new Object[entryCount][];
            System.arraycopy(records, 0, temp, 0, entryCount);
            records = temp;
            this.writer.writeSortTempFile(temp);
          } else {
            this.writer.writeSortTempFile(records);
          }
        }
      }
      double intermediateMergeCostTime =
          (System.currentTimeMillis() - intermediateMergeStartTime) / 1000.0;
      LOGGER.info("============================== Intermediate Merge of " + fileConterConst
          + " Sort Temp Files Cost Time: " + intermediateMergeCostTime + "(s)");
    } catch (Exception e) {
      LOGGER.error(e, "Problem while intermediate merging");
      isFailed = true;
    } finally {
      records = null;
      CarbonUtil.closeStreams(this.stream);
      if (null != writer) {
        writer.finish();
      }
      if (!isFailed) {
        try {
          finish();
        } catch (CarbonSortKeyAndGroupByException e) {
          LOGGER.error(e, "Problem while deleting the merge file");
        }
      } else {
        if (outPutFile.delete()) {
          LOGGER.error("Problem while deleting the merge file");
        }
      }
    }

    return null;
  }

  /**
   * This method is responsible for initializing the out stream
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void initialize() throws CarbonSortKeyAndGroupByException {
    if (!mergerParameters.isSortFileCompressionEnabled() && !mergerParameters.isPrefetch()) {
      try {
        this.stream = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(outPutFile),
                mergerParameters.getFileWriteBufferSize()));
        this.stream.writeInt(this.totalNumberOfRecords);
      } catch (FileNotFoundException e) {
        throw new CarbonSortKeyAndGroupByException("Problem while getting the file", e);
      } catch (IOException e) {
        throw new CarbonSortKeyAndGroupByException("Problem while writing the data to file", e);
      }
    } else {
      writer = TempSortFileWriterFactory.getInstance()
          .getTempSortFileWriter(mergerParameters.isSortFileCompressionEnabled(),
              mergerParameters.getDimColCount(), mergerParameters.getComplexDimColCount(),
              mergerParameters.getMeasureColCount(), mergerParameters.getNoDictionaryCount(),
              mergerParameters.getFileWriteBufferSize());
      writer.initiaize(outPutFile, totalNumberOfRecords);

      if (mergerParameters.isPrefetch()) {
        totalSize = mergerParameters.getBufferSize();
      } else {
        totalSize = mergerParameters.getSortTempFileNoOFRecordsInCompression();
      }
    }
  }

  /**
   * This method will be used to get the sorted record from file
   *
   * @return sorted record sorted record
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] getSortedRecordFromFile() throws CarbonSortKeyAndGroupByException {
    Object[] row = null;

    // poll the top object from heap
    // heap maintains binary tree which is based on heap condition that will
    // be based on comparator we are passing the heap
    // when will call poll it will always delete root of the tree and then
    // it does trickel down operation complexity is log(n)
    SortTempChunkHolder poll = this.recordHolderHeap.poll();

    // get the row from chunk
    row = poll.getRow();

    // check if there no entry present
    if (!poll.hasNext()) {
      // if chunk is empty then close the stream
      poll.close();

      // change the file counter
      --this.fileCounter;

      // reaturn row
      return row;
    }

    // read new row
    poll.readRow();

    // add to heap
    this.recordHolderHeap.add(poll);

    // return row
    return row;
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void startSorting() throws CarbonSortKeyAndGroupByException {
    LOGGER.info("Number of temp file: " + this.fileCounter);

    // create record holder heap
    createRecordHolderQueue(intermediateFiles);

    // iterate over file list and create chunk holder and add to heap
    LOGGER.info("Started adding first record from each file");

    SortTempChunkHolder sortTempFileChunkHolder = null;

    for (File tempFile : intermediateFiles) {
      // create chunk holder
      sortTempFileChunkHolder = new UnsafeSortTempFileChunkHolder(tempFile, mergerParameters);

      sortTempFileChunkHolder.readRow();
      this.totalNumberOfRecords += sortTempFileChunkHolder.numberOfRows();

      // add to heap
      this.recordHolderHeap.add(sortTempFileChunkHolder);
    }

    LOGGER.info("Heap Size" + this.recordHolderHeap.size());
  }

  /**
   * This method will be used to create the heap which will be used to hold
   * the chunk of data
   *
   * @param listFiles list of temp files
   */
  private void createRecordHolderQueue(File[] listFiles) {
    // creating record holder heap
    this.recordHolderHeap = new PriorityQueue<SortTempChunkHolder>(listFiles.length);
  }

  /**
   * This method will be used to get the sorted row
   *
   * @return sorted row
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] next() throws CarbonSortKeyAndGroupByException {
    return getSortedRecordFromFile();
  }

  /**
   * This method will be used to check whether any more element is present or
   * not
   *
   * @return more element is present
   */
  private boolean hasNext() {
    return this.fileCounter > 0;
  }

  /**
   * Below method will be used to write data to file
   *
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  private void writeDataTofile(Object[] row) throws CarbonSortKeyAndGroupByException, IOException {
    if (mergerParameters.isSortFileCompressionEnabled() || mergerParameters.isPrefetch()) {
      if (entryCount == 0) {
        records = new Object[totalSize][];
        records[entryCount++] = row;
        return;
      }

      records[entryCount++] = row;
      if (entryCount == totalSize) {
        this.writer.writeSortTempFile(records);
        entryCount = 0;
        records = new Object[totalSize][];
      }
      return;
    }
    long address = rowData.getBaseOffset();
    int dimCount = 0;
    int size = 0;
    char[] aggType = mergerParameters.getAggType();
    for (; dimCount < noDictionarycolumnMapping.length; dimCount++) {
      if (noDictionarycolumnMapping[dimCount]) {
        byte[] col = (byte[]) row[dimCount];
        CarbonUnsafe.unsafe.putShort(rowData.getBaseObject(), address + size, (short) col.length);
        size += 2;
        CarbonUnsafe.unsafe
            .copyMemory(col, CarbonUnsafe.BYTE_ARRAY_OFFSET, rowData.getBaseObject(),
                address + size, col.length);
        size += col.length;
      } else {
        CarbonUnsafe.unsafe.putInt(rowData.getBaseObject(), address + size, (int) row[dimCount]);
        size += 4;
      }
    }

    // write complex dimensions here.
    int dimensionSize = mergerParameters.getDimColCount();
    int measureSize = mergerParameters.getMeasureColCount();
    for (; dimCount < dimensionSize; dimCount++) {
      byte[] col = (byte[]) row[dimCount];
      CarbonUnsafe.unsafe.putShort(rowData.getBaseObject(), address + size, (short) col.length);
      size += 2;
      CarbonUnsafe.unsafe.copyMemory(col, CarbonUnsafe.BYTE_ARRAY_OFFSET, rowData.getBaseObject(),
          address + size, col.length);
      size += col.length;
    }
    Arrays.fill(nullSetWords, 0);
    int nullSetSize = nullSetWords.length * 8;
    int nullLoc = size;
    size += nullSetSize;
    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      Object value = row[mesCount + dimensionSize];
      if (null != value) {
        if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
          Double val = (Double) value;
          CarbonUnsafe.unsafe.putDouble(rowData.getBaseObject(), address + size, val);
          size += 8;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
          Long val = (Long) value;
          CarbonUnsafe.unsafe.putLong(rowData.getBaseObject(), address + size, val);
          size += 8;
        } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          BigDecimal val = (BigDecimal) value;
          byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
          CarbonUnsafe.unsafe.putShort(rowData.getBaseObject(), address + size,
              (short) bigDecimalInBytes.length);
          size += 2;
          CarbonUnsafe.unsafe.copyMemory(bigDecimalInBytes, CarbonUnsafe.BYTE_ARRAY_OFFSET,
              rowData.getBaseObject(), address + size, bigDecimalInBytes.length);
          size += bigDecimalInBytes.length;
        }
        UnsafeCarbonRowPage.set(nullSetWords, mesCount);
      } else {
        UnsafeCarbonRowPage.unset(nullSetWords, mesCount);
      }
    }
    CarbonUnsafe.unsafe
        .copyMemory(nullSetWords, CarbonUnsafe.LONG_ARRAY_OFFSET, rowData.getBaseObject(),
            address+nullLoc, nullSetWords.length);
    byte[] rowBytes = new byte[size];
    CarbonUnsafe.unsafe
        .copyMemory(rowData.getBaseObject(), address, rowBytes, CarbonUnsafe.BYTE_ARRAY_OFFSET,
            size);
    stream.write(rowBytes);
  }

  private void finish() throws CarbonSortKeyAndGroupByException {
    if (recordHolderHeap != null) {
      int size = recordHolderHeap.size();
      for (int i = 0; i < size; i++) {
        recordHolderHeap.poll().close();
      }
    }
    try {
      CarbonUtil.deleteFiles(intermediateFiles);
      MemoryAllocator.HEAP.free(rowData);
    } catch (CarbonUtilException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while deleting the intermediate files");
    }
  }
}

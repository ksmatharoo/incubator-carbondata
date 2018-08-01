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
package org.apache.carbondata.core.statusmanager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;

import static org.apache.carbondata.core.util.CarbonUtil.closeStreams;

public class FileBasedSegmentStore implements SegmentStore, HistorySupportSegmentStore {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(FileBasedSegmentStore.class.getName());

  private int retryCount = CarbonLockUtil
      .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
          CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);

  private int maxTimeout = CarbonLockUtil
      .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
          CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);

  @Override public List<SegmentDetailVO> getSegments(AbsoluteTableIdentifier identifier,
      List<Expression> filters) {
    try {
      LoadMetadataDetails[] details =
          readTableStatusFile(CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()));
      List<SegmentDetailVO> detailVOS = new ArrayList<>();
      for (LoadMetadataDetails detail : details) {
        detailVOS.add(SegmentManagerHelper.convertToSegmentDetailVO(detail));
      }
      return detailVOS;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public String generateSegmentIdAndInsert(AbsoluteTableIdentifier identifier,
      SegmentDetailVO segment) throws IOException {
    ICarbonLock carbonLock = getTableStatusLock(identifier);
    try {
      if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
        LOGGER.info(
            "Acquired lock for table" + identifier.uniqueName() + " for table status updation");
        LoadMetadataDetails[] details =
            readTableStatusFile(CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()));
        if (segment.getSegmentId() == null) {
          int newSegmentId = createNewSegmentId(details);
          segment.setSegmentId(String.valueOf(newSegmentId));
        }
        List<LoadMetadataDetails> listOfLoadFolderDetails = new ArrayList<>();
        Collections.addAll(listOfLoadFolderDetails, details);
        LoadMetadataDetails detail = SegmentManagerHelper.createLoadMetadataDetails(segment);
        listOfLoadFolderDetails.add(detail);
        writeLoadDetailsIntoFile(identifier, listOfLoadFolderDetails
            .toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info(
            "Table unlocked successfully after table status updation" + identifier.uniqueName());
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + identifier.uniqueName()
            + " during table status updation");
      }
    }
    return null;
  }

  @Override public boolean updateSegments(AbsoluteTableIdentifier identifier,
      List<SegmentDetailVO> detailVOS) {
    ICarbonLock carbonLock = getTableStatusLock(identifier);
    try {
      if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
        LOGGER.info(
            "Acquired lock for table" + identifier.uniqueName() + " for table status updation");
        LoadMetadataDetails[] details = readTableStatusFile(CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()));
        for (LoadMetadataDetails detail : details) {
          for (SegmentDetailVO detailVO : detailVOS) {
            if (detailVO.getSegmentId().equals(detail.getLoadName())) {
              SegmentManagerHelper.updateLoadMetadataDetails(detailVO, detail);
            }
          }
        }
        writeLoadDetailsIntoFile(identifier, details);
        return true;
      } else {
        LOGGER.error("Unable to aquire Table lock for table" + identifier.uniqueName()
            + " during table status updation");
        return false;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info(
            "Table unlocked successfully after table status updation" + identifier.uniqueName());
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + identifier.uniqueName()
            + " during table status updation");
      }
    }
  }

  @Override
  public boolean commitTransaction(List<AbsoluteTableIdentifier> identifiers, String uuid) {
    List<ICarbonLock> locks = new ArrayList<>(identifiers.size());
    for (AbsoluteTableIdentifier identifier : identifiers) {
      locks.add(getTableStatusLock(identifier));
    }

    boolean locked = false;
    try {
      for (ICarbonLock lock : locks) {
        locked = lock.lockWithRetries(retryCount, maxTimeout);
        if (!locked) {
          break;
        }
      }
      if (locked) {
        Map<AbsoluteTableIdentifier, LoadMetadataDetails[]> identifierMap = new HashMap<>();
        Map<AbsoluteTableIdentifier, List<String>> identifierSegmentMap = new HashMap<>();
        for (AbsoluteTableIdentifier identifier : identifiers) {
          LoadMetadataDetails[] details = readTableStatusFile(
              CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()));
          identifierMap.put(identifier, details);
          List<String> segments = new ArrayList<>();
          for (LoadMetadataDetails detail : details) {
            if (detail.getTransactionId() != null && detail.getTransactionId().equals(uuid)) {
              detail.setTransactionId(null);
              segments.add(detail.getLoadName());
            }
          }
          identifierSegmentMap.put(identifier, segments);
        }
        List<AbsoluteTableIdentifier> finished = new ArrayList<>();
        for (Map.Entry<AbsoluteTableIdentifier, LoadMetadataDetails[]> entry : identifierMap
            .entrySet()) {
          try {
            writeLoadDetailsIntoFile(entry.getKey(), entry.getValue());
          } catch (Exception e) {
            LOGGER.error(e);
            break;
          }
          finished.add(entry.getKey());
        }

        if (finished.size() != identifiers.size()) {
          // Roll back
          for (AbsoluteTableIdentifier identifier : finished) {
            List<String> list = identifierSegmentMap.get(identifier);
            if (list != null && list.size() > 0) {
              LoadMetadataDetails[] details = identifierMap.get(identifier);
              for (LoadMetadataDetails detail : details) {
                if (list.contains(detail.getLoadName())) {
                  detail.setTransactionId(uuid);
                }
              }
              writeLoadDetailsIntoFile(identifier, details);
            }
          }
        }
        return true;
      }
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      for (ICarbonLock lock : locks) {
        lock.unlock();
      }
    }
  }

  @Override public void deleteSegments(AbsoluteTableIdentifier identifier) {

  }

  /**
   * This method will get the max segment id
   *
   * @param loadMetadataDetails
   * @return
   */
  private int getMaxSegmentId(LoadMetadataDetails[] loadMetadataDetails) {
    int newSegmentId = -1;
    for (int i = 0; i < loadMetadataDetails.length; i++) {
      try {
        int loadCount = Integer.parseInt(loadMetadataDetails[i].getLoadName());
        if (newSegmentId < loadCount) {
          newSegmentId = loadCount;
        }
      } catch (NumberFormatException ne) {
        // this case is for compacted folders. For compacted folders Id will be like 0.1, 2.1
        // consider a case when 12 loads are completed and after that major compaction is triggered.
        // In this case new compacted folder will be created with name 12.1 and after query time
        // out all the compacted folders will be deleted and entry will also be removed from the
        // table status file. In that case also if a new load comes the new segment Id assigned
        // should be 13 and not 0
        String loadName = loadMetadataDetails[i].getLoadName();
        if (loadName.contains(".")) {
          int loadCount = Integer.parseInt(loadName.split("\\.")[0]);
          if (newSegmentId < loadCount) {
            newSegmentId = loadCount;
          }
        }
      }
    }
    return newSegmentId;
  }

  /**
   * This method will create new segment id
   *
   * @param loadMetadataDetails
   * @return
   */
  private int createNewSegmentId(LoadMetadataDetails[] loadMetadataDetails) {
    int newSegmentId = getMaxSegmentId(loadMetadataDetails);
    newSegmentId++;
    return newSegmentId;
  }

  private ICarbonLock getTableStatusLock(AbsoluteTableIdentifier identifier) {
    return CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.TABLE_STATUS_LOCK);
  }

  private LoadMetadataDetails[] readTableStatusFile(String tableStatusPath) throws IOException {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    LoadMetadataDetails[] listOfLoadFolderDetailsArray;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(tableStatusPath);

    try {
      if (!FileFactory.isFileExist(tableStatusPath, FileFactory.getFileType(tableStatusPath))) {
        return new LoadMetadataDetails[0];
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      listOfLoadFolderDetailsArray =
          gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
    } catch (IOException e) {
      LOGGER.error(e, "Failed to read metadata of load");
      throw e;
    } finally {
      closeStreams(buffReader, inStream, dataInputStream);
    }

    // if listOfLoadFolderDetailsArray is null, return empty array
    if (null == listOfLoadFolderDetailsArray) {
      return new LoadMetadataDetails[0];
    }

    return listOfLoadFolderDetailsArray;
  }

  private void writeHistoryLoadDetailsIntoFile(AbsoluteTableIdentifier identifier,
      LoadMetadataDetails[] listOfLoadFolderDetailsArray) throws IOException {
    String dataLoadLocation =
        CarbonTablePath.getTableStatusHistoryFilePath(identifier.getTablePath());
    writeStatusFile(listOfLoadFolderDetailsArray, dataLoadLocation);
  }

  /**
   * writes load details into a given file at @param dataLoadLocation
   *
   * @param identifier
   * @param listOfLoadFolderDetailsArray
   * @throws IOException
   */
  private void writeLoadDetailsIntoFile(AbsoluteTableIdentifier identifier,
      LoadMetadataDetails[] listOfLoadFolderDetailsArray) throws IOException {
    String dataLoadLocation = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
    writeStatusFile(listOfLoadFolderDetailsArray, dataLoadLocation);

  }

  private void writeStatusFile(LoadMetadataDetails[] listOfLoadFolderDetailsArray,
      String dataLoadLocation) throws IOException {
    AtomicFileOperations writeOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(dataLoadLocation);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the metadata file.

    try {
      dataOutputStream = writeOperation.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetailsArray);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOGGER.error("Error message: " + ioe.getLocalizedMessage());
      writeOperation.setFailed();
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      writeOperation.close();
    }
  }

  @Override public List<SegmentDetailVO> getHistorySegments(AbsoluteTableIdentifier identifier,
      List<Expression> filters) {
    List<SegmentDetailVO> detailVOS = new ArrayList<>();
    try {
      LoadMetadataDetails[] details = readTableStatusFile(
          CarbonTablePath.getTableStatusHistoryFilePath(identifier.getTablePath()));
      for (LoadMetadataDetails detail : details) {
        detailVOS.add(SegmentManagerHelper.convertToSegmentDetailVO(detail));
      }
    } catch (IOException e) {
      LOGGER.error(e);
    }
    return detailVOS;
  }

  @Override
  public void moveHistorySegments(AbsoluteTableIdentifier identifier, boolean isForceDeletion)
      throws IOException {
    int invisibleSegmentPreserveCnt =
        CarbonProperties.getInstance().getInvisibleSegmentPreserveCount();
    ICarbonLock carbonLock = getTableStatusLock(identifier);
    try {
      if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
        LoadMetadataDetails[] details =
            readTableStatusFile(CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()));
        int maxSegmentId = getMaxSegmentId(details);
        int invisibleSegmentCnt = countInvisibleSegments(details, maxSegmentId);
        // if execute command 'clean files' or the number of invisible segment info
        // exceeds the value of 'carbon.invisible.segments.preserve.count',
        // it need to append the invisible segment list to 'tablestatus.history' file.
        if (isForceDeletion || (invisibleSegmentCnt > invisibleSegmentPreserveCnt)) {
          List<LoadMetadataDetails> visibleSegments = new ArrayList<>();
          List<LoadMetadataDetails> invisibleSegments = new ArrayList<>();
          separateVisibleAndInvisibleSegments(details, maxSegmentId, visibleSegments,
              invisibleSegments);
          LoadMetadataDetails[] oldLoadHistoryList =
              readLoadHistoryMetadata(CarbonTablePath.getMetadataPath(identifier.getTablePath()));
          LoadMetadataDetails[] newLoadHistoryList = appendLoadHistoryList(oldLoadHistoryList,
              invisibleSegments.toArray(new LoadMetadataDetails[invisibleSegments.size()]));
          writeLoadDetailsIntoFile(identifier,
              visibleSegments.toArray(new LoadMetadataDetails[visibleSegments.size()]));
          writeHistoryLoadDetailsIntoFile(identifier, newLoadHistoryList);
        }
      }
    } finally {
      carbonLock.unlock();
    }
  }

  /**
   * Get the number of invisible segment info from segment info list.
   */
  private int countInvisibleSegments(LoadMetadataDetails[] segmentList, int maxSegmentId) {
    int invisibleSegmentCnt = 0;
    if (segmentList.length != 0) {
      for (LoadMetadataDetails eachSeg : segmentList) {
        // can not remove segment 0, there are some info will be used later
        // for example: updateStatusFileName
        // also can not remove the max segment id,
        // otherwise will impact the generation of segment id
        if (!eachSeg.getLoadName().equalsIgnoreCase("0") && !eachSeg.getLoadName()
            .equalsIgnoreCase(String.valueOf(maxSegmentId)) && eachSeg.getVisibility()
            .equalsIgnoreCase("false")) {
          invisibleSegmentCnt += 1;
        }
      }
    }
    return invisibleSegmentCnt;
  }

  /**
   * Separate visible and invisible segments into two array.
   */
  private void separateVisibleAndInvisibleSegments(LoadMetadataDetails[] list, int maxSegmentId,
      List<LoadMetadataDetails> visibleSegments, List<LoadMetadataDetails> invisibleSegments) {
    for (int i = 0; i < list.length; i++) {
      LoadMetadataDetails newSegment = list[i];
      if (newSegment.getLoadName().equalsIgnoreCase("0") || newSegment.getLoadName()
          .equalsIgnoreCase(String.valueOf(maxSegmentId))) {
        visibleSegments.add(newSegment);
      } else if ("false".equalsIgnoreCase(newSegment.getVisibility())) {
        invisibleSegments.add(newSegment);
      } else {
        visibleSegments.add(newSegment);
      }
    }
  }

  /**
   * Return an array containing all invisible segment entries in appendList and historyList.
   */
  private LoadMetadataDetails[] appendLoadHistoryList(LoadMetadataDetails[] historyList,
      LoadMetadataDetails[] appendList) {
    int historyListLen = historyList.length;
    int appendListLen = appendList.length;
    int newListLen = historyListLen + appendListLen;
    LoadMetadataDetails[] newList = new LoadMetadataDetails[newListLen];
    int newListIdx = 0;
    for (int i = 0; i < historyListLen; i++) {
      newList[newListIdx] = historyList[i];
      newListIdx++;
    }
    for (int i = 0; i < appendListLen; i++) {
      newList[newListIdx] = appendList[i];
      newListIdx++;
    }
    return newList;
  }

  /**
   * This method reads the load history metadata file
   *
   * @param metadataFolderPath
   * @return
   */
  private LoadMetadataDetails[] readLoadHistoryMetadata(String metadataFolderPath) {
    String metadataFileName = metadataFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonTablePath.TABLE_STATUS_HISTORY_FILE;
    try {
      return readTableStatusFile(metadataFileName);
    } catch (IOException e) {
      return new LoadMetadataDetails[0];
    }
  }

  @Override
  public long getTableStatusLastModifiedTime(AbsoluteTableIdentifier identifier)
      throws IOException {
    String tableStatusPath = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
    if (!FileFactory.isFileExist(tableStatusPath, FileFactory.getFileType(tableStatusPath))) {
      return 0L;
    } else {
      return FileFactory.getCarbonFile(tableStatusPath, FileFactory.getFileType(tableStatusPath))
          .getLastModifiedTime();
    }
  }
}

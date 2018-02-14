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

package org.apache.carbondata.core.indexstore;

import java.util.Objects;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * Class holds the absoluteTableIdentifier and segment to uniquely identify a segment
 */
public class TableBlockIndexUniqueIdentifier {

  private String indexFilePath;

  private String indexFileName;

  private String mergeIndexFileName;

  public TableBlockIndexUniqueIdentifier(String indexFilePath, String indexFileName,
      String mergeIndexFileName) {
    this.indexFilePath = indexFilePath;
    this.indexFileName = indexFileName;
    this.mergeIndexFileName = mergeIndexFileName;
  }

  /**
   * method returns the id to uniquely identify a key
   *
   * @return
   */
  public String getUniqueTableSegmentIdentifier() {
    return indexFilePath + CarbonCommonConstants.FILE_SEPARATOR + indexFileName;
  }

  public String getIndexFilePath() {
    return indexFilePath;
  }

  public String getIndexFileName() {
    return indexFileName;
  }

  public String getMergeIndexFileName() {
    return mergeIndexFileName;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableBlockIndexUniqueIdentifier that = (TableBlockIndexUniqueIdentifier) o;
    return Objects.equals(indexFilePath, that.indexFilePath) && Objects
        .equals(indexFileName, that.indexFileName) && Objects
        .equals(mergeIndexFileName, that.mergeIndexFileName);
  }

  @Override public int hashCode() {
    return Objects.hash(indexFilePath, indexFileName, mergeIndexFileName);
  }
}

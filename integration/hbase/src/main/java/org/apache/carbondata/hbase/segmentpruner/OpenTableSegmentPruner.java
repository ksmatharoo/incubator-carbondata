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
package org.apache.carbondata.hbase.segmentpruner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.carbondata.core.indexstore.PrunedSegmentInfo;
import org.apache.carbondata.core.indexstore.SegmentPrunerImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo;

public class OpenTableSegmentPruner extends SegmentPrunerImpl {

  @Override
  public List<PrunedSegmentInfo> pruneSegment(CarbonTable table, Expression filterExp,
      String[] inputSegments, String[] excludeSegment) {
    List<PrunedSegmentInfo> prunedSegmentInfos =
        super.pruneSegment(table, filterExp, inputSegments, excludeSegment);
    List<PrunedSegmentInfo> hbase = prunedSegmentInfos.stream().filter(
        seg -> seg.getSegment().getLoadMetadataDetails().getFileFormat().toString()
            .equalsIgnoreCase("hbase")).collect(Collectors.toList());
    if (hbase.size() > 0) {
      try {
        hbase.get(0).getSegmentFile()
            .setSegmentMetaDataInfo(new SegmentMetaDataInfo(new HashMap<>()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return hbase;
    } else {
      return prunedSegmentInfos;
    }
  }
}

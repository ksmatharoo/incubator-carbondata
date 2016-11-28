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
package org.apache.carbondata.processing.newflow.sort.unsafe;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Manages memory for instance.
 */
public class UnsafeMemoryManager {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeMemoryManager.class.getName());

  static {
    int size = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB,
            CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB_DEFAULT));
    INSTANCE = new UnsafeMemoryManager(size);
  }

  public static final UnsafeMemoryManager INSTANCE;

  private int memoryInMB;

  private int memoryUsed;

  private UnsafeMemoryManager(int memoryInMB) {
    this.memoryInMB = memoryInMB;
  }

  public synchronized boolean allocateMemory(int memoryInMBRequested) {
    if (memoryUsed + memoryInMBRequested < memoryInMB) {
      memoryUsed += memoryInMBRequested;
      LOGGER.info("Total memory used "+ memoryUsed + " memory left "+(getAvailableMemory()));
      return true;
    }
    return false;
  }

  public synchronized void freeMemory(int memoryInMBtoFree) {
    memoryUsed -= memoryInMBtoFree;
    memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
    LOGGER.info("Memory released, memory used "+ memoryUsed
        + " memory left "+(getAvailableMemory()));
  }

  public synchronized int getAvailableMemory() {
    return memoryInMB - memoryUsed;
  }
}

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

import org.apache.carbondata.processing.newflow.sort.unsafe.memory.MemoryAllocator;
import org.apache.carbondata.processing.newflow.sort.unsafe.memory.MemoryBlock;

import org.apache.spark.unsafe.Platform;

/**
 * Holds the pointers for rows.
 */
public class IntPointerBuffer {

  private int length;

  private int actualSize;

  private int totalSize;

  private MemoryAllocator allocator;

  private int[] pointerBlock;

  private MemoryBlock baseBlock;

  public IntPointerBuffer(int sizeInMB, boolean unsafe) {
    // TODO can be configurable, it is initial size and it can grow automatically.
    this.length = 100000;
    this.totalSize = sizeInMB;
    if (unsafe) {
      allocator = MemoryAllocator.UNSAFE;
    } else {
      allocator = MemoryAllocator.HEAP;
    }
    pointerBlock = new int[length];
    baseBlock = allocator.allocate(sizeInMB * 1024 * 1024);
  }

  public IntPointerBuffer(int length) {
    this.length = length;
    this.baseBlock = baseBlock;
    pointerBlock = new int[length];
  }

  /**
   * Fill this all with 0.
   */
  private void zeroOut(MemoryBlock memoryBlock) {
    long length = memoryBlock.size() / 8;
    long maSize = memoryBlock.getBaseOffset() + length * 8;
    for (long off = memoryBlock.getBaseOffset(); off < maSize; off += 8) {
      Platform.putLong(memoryBlock.getBaseObject(), off, 0);
    }
  }

  public void set(int index, int value) {
    pointerBlock[index] = value;
  }

  public void set(int value) {
    ensureMemory();
    pointerBlock[actualSize] = value;
    actualSize++;
  }

  /**
   * Returns the value at position {@code index}.
   */
  public int get(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    return pointerBlock[index];
  }

  public int getActualSize() {
    return actualSize;
  }

  public MemoryBlock getBaseBlock() {
    return baseBlock;
  }

  public int[] getPointerBlock() {
    return pointerBlock;
  }

  private void ensureMemory() {
    if (actualSize >= length) {
      // Expand by quarter, may be we can correct the logic later
      int localLength = length + (int) (length * (0.25));
      int[] memoryAddress = new int[localLength];
      System.arraycopy(pointerBlock, 0, memoryAddress, 0, length);
      pointerBlock = memoryAddress;
      length = localLength;
    }
  }

  public int getTotalSize() {
    return totalSize;
  }

  public void freeMemory() {
    pointerBlock = null;
    if (baseBlock != null) {
      allocator.free(baseBlock);
    }
  }
}

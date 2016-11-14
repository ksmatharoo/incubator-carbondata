package org.apache.carbondata.processing.newflow.sort.impl;

import java.util.Iterator;

import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.newflow.sort.Sorter;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;

/**
 * Created by root1 on 14/11/16.
 */
public class UnsafeParallelReadMergeSorterImpl implements Sorter {

  @Override public void initialize(SortParameters sortParameters) {

  }

  @Override public Iterator<CarbonRowBatch>[] sort(Iterator<CarbonRowBatch>[] iterators)
      throws CarbonDataLoadingException {
    return new Iterator[0];
  }

  @Override public void close() {

  }
}

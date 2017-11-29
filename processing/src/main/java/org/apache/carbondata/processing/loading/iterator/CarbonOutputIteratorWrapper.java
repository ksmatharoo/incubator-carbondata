package org.apache.carbondata.processing.loading.iterator;

import java.util.concurrent.ArrayBlockingQueue;

import org.apache.carbondata.common.CarbonIterator;

public class CarbonOutputIteratorWrapper extends CarbonIterator<String[]> {

  private boolean close = false;

  private ArrayBlockingQueue<String[]> queue = new ArrayBlockingQueue<String[]>(100);

  public void write(String[] row) throws InterruptedException {
    queue.put(row);
  }

  @Override public boolean hasNext() {
    return !queue.isEmpty() && !close;
  }

  @Override public String[] next() {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void close() {
    close = true;
  }
}

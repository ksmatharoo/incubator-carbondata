package org.apache.carbondata.tranaction;

import java.util.List;

import org.apache.carbondata.core.datamap.Segment;

public class DeleteActionMetadata {
  private List<Segment> segmentsToBeDeleted;
  private long deletedRowCount;

  public List<Segment> getSegmentsToBeDeleted() {
    return segmentsToBeDeleted;
  }

  public void setSegmentsToBeDeleted(List<Segment> segmentsToBeDeleted) {
    this.segmentsToBeDeleted = segmentsToBeDeleted;
  }

  public long getDeletedRowCount() {
    return deletedRowCount;
  }

  public void setDeletedRowCount(long deletedRowCount) {
    this.deletedRowCount = deletedRowCount;
  }
}

package org.apache.carbondata.core.util;

import org.apache.carbondata.format.DataChunk3;

public class CompressedColumnPageVo {

  public DataChunk3 dataChunk3;

  public byte[] data;

  public CompressedColumnPageVo(DataChunk3 dataChunk3, byte[] data) {
    this.dataChunk3 = dataChunk3;
    this.data = data;
  }
}

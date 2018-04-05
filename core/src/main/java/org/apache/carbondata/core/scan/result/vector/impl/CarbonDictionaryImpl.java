package org.apache.carbondata.core.scan.result.vector.impl;

import org.apache.carbondata.core.datastore.chunk.store.DimensionDataChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.safe.SafeVariableLengthDimensionDataChunkStore;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;

public class CarbonDictionaryImpl implements CarbonDictionary {

  private byte[][] dictionary;

  public CarbonDictionaryImpl(byte[][] dictionary) {
    this.dictionary = dictionary;
  }

  @Override public byte[][] getRleData() {
    return dictionary;
  }

}

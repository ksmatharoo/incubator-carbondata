package org.apache.carbondata.processing.newflow.row;

import java.math.BigDecimal;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;

/**
 * Created by root1 on 14/11/16.
 */
public class UnsafeCarbonRow implements CarbonRow {

  private DataType[] dataTypes;

  public UnsafeCarbonRow(DataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  @Override public Object[] getData() {
    return new Object[0];
  }

  @Override public void setData(Object[] data) {

  }

  @Override public int getInt(int ordinal) {
    return 0;
  }

  @Override public long getLong(int ordinal) {
    return 0;
  }

  @Override public float getFloat(int ordinal) {
    return 0;
  }

  @Override public double getDouble(int ordinal) {
    return 0;
  }

  @Override public BigDecimal getDecimal(int ordinal) {
    return null;
  }

  @Override public String getString(int ordinal) {
    return null;
  }

  @Override public Object getObject(int ordinal) {
    return null;
  }

  @Override public byte[] getBinary(int ordinal) {
    return new byte[0];
  }

  @Override public Object[] getObjectArray(int ordinal) {
    return new Object[0];
  }

  @Override public int[] getIntArray(int ordinal) {
    return new int[0];
  }

  @Override public void update(Object value, int ordinal) {

  }

  @Override public CarbonRow getCopy() {
    return null;
  }
}

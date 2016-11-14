package org.apache.carbondata.processing.newflow.row;

import java.math.BigDecimal;

/**
 * This row class is used to transfer the row data from one step to other step
 */
public interface CarbonRow {

  Object[] getData();

  void setData(Object[] data);

  int getInt(int ordinal);

  long getLong(int ordinal);

  float getFloat(int ordinal);

  double getDouble(int ordinal);

  BigDecimal getDecimal(int ordinal);

  String getString(int ordinal);

  Object getObject(int ordinal);

  byte[] getBinary(int ordinal);

  Object[] getObjectArray(int ordinal);

  int[] getIntArray(int ordinal);

  void update(Object value, int ordinal);

  CarbonRow getCopy();
}

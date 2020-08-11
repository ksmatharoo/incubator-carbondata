package org.apache.carbondata.hbase;

public class HBaseUtil {

  private HBaseUtil() {

  }

  public static String updateColumnFamily(String columnFamilyName, String columnName) {
    return HBaseConstants.BACKTICK + columnFamilyName + HBaseConstants.DOT + columnName
        + HBaseConstants.BACKTICK;
  }
}

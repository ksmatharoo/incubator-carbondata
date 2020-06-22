package org.apache.spark.carbondata.hbasestreaming

case class IntKeyRecord(
    col0: Integer,
    col1: String,
    col2: Int)

object IntKeyRecord {
  def apply(i: Int): IntKeyRecord = {
    IntKeyRecord(if (i % 2 == 0) {
      i
    } else {
      -i
    },
      s"String$i extra",
      i)
  }
}

package org.apache.spark.spatialjoin.extractor

import java.sql.Timestamp

/**
  * @author wangrubin3
  **/
trait STExtractor[T] extends GeomExtractor[T] {
  def startTime(row: T): Timestamp

  def endTime(row: T): Timestamp
}

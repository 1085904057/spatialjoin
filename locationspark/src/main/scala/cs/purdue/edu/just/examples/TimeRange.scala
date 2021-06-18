package cs.purdue.edu.just.examples

import java.sql.Timestamp

case class TimeRange(startTime: Timestamp, endTime: Timestamp) {
  def getDeltaTime(): Long = endTime.getTime - startTime.getTime
  def getDeltaTime(threshold: Long): Long = 2 * threshold
}
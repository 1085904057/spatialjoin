package org.apache.spark.spatialjoin.utils

import java.sql.Timestamp

/**
  * @author wangrubin3
  **/
object TimeUtils {
  val timeOrdering =  new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp):Int = {
      x.compareTo(y)
    }
  }

  def isIntersects(range1: (Timestamp, Timestamp), range2: (Timestamp, Timestamp)): Boolean = {
    !(range1._1.after(range2._2) || range1._2.before(range2._1))
  }

  def timeReferPoint(range1: (Timestamp, Timestamp), range2: (Timestamp, Timestamp)): Timestamp = {
    if (isIntersects(range1, range2)) {
      if (range1._1.before(range2._1)) range2._1 else range1._1
    } else null
  }

  def expandTimeRange(timeRange: (Timestamp, Timestamp), deltaMilli: Long): (Timestamp, Timestamp) = {
    (new Timestamp(timeRange._1.getTime - deltaMilli), new Timestamp(timeRange._2.getTime + deltaMilli))
  }
}

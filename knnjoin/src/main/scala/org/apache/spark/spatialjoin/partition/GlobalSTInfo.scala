package org.apache.spark.spatialjoin.partition

import java.sql.Timestamp

import org.apache.spark.spatialjoin.extractor.STExtractor
import org.apache.spark.rdd.RDD

/**
  * @author wangrubin3
  **/
class GlobalSTInfo extends GlobalSpatialInfo {
  private var startTime: Timestamp = _
  private var endTime: Timestamp = _

  def addTime(timeRange: (Timestamp, Timestamp)): GlobalSTInfo = {
    this.expandTime(timeRange._1, timeRange._2)
    this
  }

  def combine(other: GlobalSTInfo): GlobalSTInfo = {
    if (other.startTime != null) {
      super.combine(other)
      this.expandTime(other.startTime, other.endTime)
    }
    this
  }

  def getTimeRange: (Timestamp, Timestamp) = {
    (this.startTime, this.endTime)
  }

  private def expandTime(start: Timestamp, end: Timestamp): Unit = {
    assert(!start.after(end)) //todo: remove
    if (null == this.startTime) {
      this.startTime = start
      this.endTime = end
    } else {
      if (start.before(this.startTime)) this.startTime = start
      if (end.after(this.endTime)) this.endTime = end
    }
  }
}

object GlobalSTInfo {
  def doStatistic[T](rdd: RDD[T], extractor: STExtractor[T]): GlobalSTInfo = {
    val add = (globalInfo: GlobalSTInfo, row: T) => {
      globalInfo.addGeom(extractor.geom(row))
      globalInfo.addTime((extractor.startTime(row), extractor.endTime(row)))
    }
    val combine = (globalInfo: GlobalSTInfo, other: GlobalSTInfo) => globalInfo.combine(other)
    rdd.aggregate(new GlobalSTInfo)(add, combine)
  }
}
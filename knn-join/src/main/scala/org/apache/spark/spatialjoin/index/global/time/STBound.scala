package org.apache.spark.spatialjoin.index.global.time

import java.sql.Timestamp

import org.locationtech.jts.geom.Envelope

/**
  * @author wangrubin3
  **/
case class STBound(env: Envelope, startTime: Timestamp, endTime: Timestamp) extends Serializable {
  def contains(timeReferPoint: Timestamp): Boolean = {
    !(timeReferPoint.before(startTime) || timeReferPoint.after(endTime))
  }

  def contains(start: Timestamp, end: Timestamp): Boolean = {
    start.after(this.startTime) && end.before(this.endTime)
  }

  def contains(x: Double, y: Double): Boolean = {
    env.contains(x,y)
    //GeomUtils.halfIntersect(env, x, y)
  }

  def contains(otherEnv: Envelope): Boolean = {
    env.contains(otherEnv)
  }
}

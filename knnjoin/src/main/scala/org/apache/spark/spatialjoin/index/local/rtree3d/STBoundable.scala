package org.apache.spark.spatialjoin.index.local.rtree3d

import java.sql.Timestamp

import org.locationtech.jts.geom.Envelope

/**
  * @author wangrubin3
  **/
trait STBoundable {
  def getMinTime: Timestamp

  def getMaxTime: Timestamp

  def getBound: Envelope

  def centreT: Timestamp = new Timestamp((getMinTime.getTime + getMaxTime.getTime) / 2)

  def centreX: Double = (getBound.getMinX + getBound.getMaxX) / 2d

  def centreY: Double = (getBound.getMinY + getBound.getMaxY) / 2d
}

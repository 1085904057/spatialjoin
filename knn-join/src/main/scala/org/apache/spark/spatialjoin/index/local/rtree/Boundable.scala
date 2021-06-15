package org.apache.spark.spatialjoin.index.local.rtree

import org.locationtech.jts.geom.Envelope

/**
  * @author wangrubin3
  **/
trait Boundable {
  def getBound: Envelope

  def centreX: Double = (getBound.getMinX + getBound.getMaxX) / 2d

  def centreY: Double = (getBound.getMinY + getBound.getMaxY) / 2d
}

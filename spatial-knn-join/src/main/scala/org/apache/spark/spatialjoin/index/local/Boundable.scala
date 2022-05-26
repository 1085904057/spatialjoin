package org.apache.spark.spatialjoin.index.local

import org.locationtech.jts.geom.Envelope

/**
 * @author wangrubin
 */
private[local] trait Boundable {
  def getBound: Envelope

  def centreX: Double = (getBound.getMinX + getBound.getMaxX) / 2d

  def centreY: Double = (getBound.getMinY + getBound.getMaxY) / 2d
}

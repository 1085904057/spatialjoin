package org.apache.spark.spatialjoin.index.local

import org.locationtech.jts.geom.{Envelope, Geometry}

/**
 * @author wangrubin
 */
private[local] class ItemBoundable(itemEnv: Envelope, item: Geometry) extends Boundable {
  override def getBound: Envelope = this.itemEnv

  def getItem: Geometry = this.item
}

package org.apache.spark.spatialjoin.index.local.rtree

import org.apache.spark.spatialjoin.extractor.GeomExtractor
import org.locationtech.jts.geom.{Envelope, Geometry}

/**
  * @author wangrubin3
  **/
class ItemBoundable[T](item: T, extractor: GeomExtractor[T]) extends Boundable {
  override def getBound: Envelope = extractor.geom(item).getEnvelopeInternal

  def getItem: T = this.item

  def getGeom: Geometry = extractor.geom(item)
}

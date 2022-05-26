package org.apache.spark.spatialjoin.index.local.rtree3d

import java.sql.Timestamp

import org.apache.spark.spatialjoin.extractor.STExtractor
import org.locationtech.jts.geom.{Envelope, Geometry}

/**
  * @author wangrubin3
  **/
class STItemBoundable[T](item: T, extractor: STExtractor[T]) extends STBoundable {
  override def getBound: Envelope = extractor.geom(item).getEnvelopeInternal

  def getItem: T = this.item

  def getGeom: Geometry = extractor.geom(item)

  override def getMinTime: Timestamp = extractor.startTime(item)

  override def getMaxTime:Timestamp = extractor.endTime(item)
}

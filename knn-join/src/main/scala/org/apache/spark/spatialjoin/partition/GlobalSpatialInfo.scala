package org.apache.spark.spatialjoin.partition

import org.apache.spark.spatialjoin.extractor.GeomExtractor
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Envelope, Geometry}

/**
  * @author wangrubin3
  **/
class GlobalSpatialInfo extends Serializable {
  private val env: Envelope = new Envelope()
  private var count: Long = 0L

  def addGeom(geom: Geometry): GlobalSpatialInfo = {
    env.expandToInclude(geom.getEnvelopeInternal)
    count += 1
    this
  }

  def combine(other: GlobalSpatialInfo): GlobalSpatialInfo = {
    env.expandToInclude(other.env)
    count += other.count
    this
  }

  def getEnv: Envelope = {
    env
  }

  def getCount: Long = {
    count
  }
}

object GlobalSpatialInfo {
  def doStatistic[T](rdd: RDD[T], extractor: GeomExtractor[T]): GlobalSpatialInfo = {
    val add = (globalInfo: GlobalSpatialInfo, row: T) => globalInfo.addGeom(extractor.geom(row))
    val combine = (globalInfo: GlobalSpatialInfo, other: GlobalSpatialInfo) => globalInfo.combine(other)
    rdd.aggregate(new GlobalSpatialInfo)(add, combine)
  }
}
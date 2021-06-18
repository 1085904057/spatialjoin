package org.apache.spark.spatialjoin.extractor

import org.locationtech.jts.geom.Geometry

/**
  * @author wangrubin3
  **/
trait GeomExtractor[T] extends Serializable {
  def geom(row: T): Geometry
}

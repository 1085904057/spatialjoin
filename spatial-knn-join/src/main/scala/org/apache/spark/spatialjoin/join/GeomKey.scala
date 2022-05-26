package org.apache.spark.spatialjoin.join

import org.locationtech.jts.geom.Geometry

/**
 * @author wangrubin
 */
class GeomKey(geom: Geometry) extends Serializable {
  private var id: Long = _

  def getGeom: Geometry = this.geom

  def getId: Long = this.id

  def bindId(id: Long): GeomKey = {
    this.id = id
    this
  }

  override def hashCode(): Int = this.id.hashCode()

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: GeomKey =>
        this.id.equals(other.id)
      case _ => false
    }
  }
}

case class GeomKeyWithParam( geom: Geometry, partitionId: Int,
                       expandDist: Double, shrinkK: Int) extends GeomKey(geom) {

  def getPartitionId: Int = this.partitionId

  def getExpandDist: Double = this.expandDist

  def getShrinkK: Int = shrinkK
}

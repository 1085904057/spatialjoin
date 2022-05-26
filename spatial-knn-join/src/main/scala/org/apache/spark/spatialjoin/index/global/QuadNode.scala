package org.apache.spark.spatialjoin.index.global

import org.apache.spark.spatialjoin.utils.GeomUtils
import org.locationtech.jts.geom.{Envelope, Point}

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangrubin
 */
class QuadNode(env: Envelope) extends IndexNode(env) {
  /**
    * centre point of current node
    **/
  protected val centre: Point = GeomUtils.getCentre(env)
  /**
    * sub nodes
    **/
  private val subnodes = new Array[QuadNode](4)

  override def split(maxNumPerPartition: Int, samples: Array[Envelope], k: Int): Array[IndexNode] = {
    val partitionCollector = new ArrayBuffer[QuadNode]()
    val maxNum = Math.max(maxNumPerPartition, k * 4)
    split(maxNum, samples, partitionCollector, 0)
    partitionCollector.toArray
  }

  private def split(maxNumPerPartition: Int, samples: Array[Envelope],
                    partitionCollector: ArrayBuffer[QuadNode], level: Int): Unit = {

    if (samples.length >= maxNumPerPartition && level < 15) {
      for (index <- 0 until 4) {
        assert(null == subnodes(index))
        subnodes(index) = new QuadNode(createSubEnv(index))
        val subEnv = subnodes(index).env
        val subSamples = for (sample <- samples if subEnv.intersects(sample)) yield sample
        subnodes(index).split(maxNumPerPartition, subSamples, partitionCollector, level + 1)
      }
    } else {
      this.partitionId = partitionCollector.size
      partitionCollector += this
    }
  }

  private def createSubEnv(index: Int): Envelope = {
    var minx = 0.0
    var maxx = 0.0
    var miny = 0.0
    var maxy = 0.0

    index match {
      case 0 =>
        minx = env.getMinX
        maxx = centre.getX
        miny = env.getMinY
        maxy = centre.getY
      case 1 =>
        minx = centre.getX
        maxx = env.getMaxX
        miny = env.getMinY
        maxy = centre.getY
      case 2 =>
        minx = env.getMinX
        maxx = centre.getX
        miny = centre.getY
        maxy = env.getMaxY
      case 3 =>
        minx = centre.getX
        maxx = env.getMaxX
        miny = centre.getY
        maxy = env.getMaxY
    }
    new Envelope(minx, maxx, miny, maxy)
  }

  override protected def getId(point: Point): Int = {
    if (partitionId != -1) return this.partitionId

    val subIndex = if (point.getX > centre.getX) {
      if (point.getY > centre.getY) 3 else 1
    } else {
      if (point.getY > centre.getY) 2 else 0
    }
    subnodes(subIndex).getId(point)
  }

  override protected def getNearestId(point: Point, k: Int): Int = {
    if (partitionId != -1) return partitionId

    val subIndex = if (point.getX > centre.getX) {
      if (point.getY > centre.getY) 3 else 1
    } else {
      if (point.getY > centre.getY) 2 else 0
    }

    if (subnodes(subIndex).partitionId == -1) {
      subnodes(subIndex).getNearestId(point, k)
    } else if (subnodes(subIndex).num >= k) {
      subnodes(subIndex).partitionId
    } else {
      val subNode = subnodes
        .find(node => node.num >= k || node.partitionId == -1)
        .getOrElse(throw new IllegalArgumentException("does't find nearest leaf node"))
      subNode.getNearestId(point, k)
    }
  }

  override protected def collectIds(bound: Envelope, idCollector: ArrayBuffer[Int]): Unit = {
    if (this.env.intersects(bound)) {
      if (this.partitionId != -1) idCollector += partitionId
      else subnodes.foreach(_.collectIds(bound, idCollector))
    }
  }
}
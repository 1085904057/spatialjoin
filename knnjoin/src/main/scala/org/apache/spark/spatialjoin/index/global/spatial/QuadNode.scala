package org.apache.spark.spatialjoin.index.global.spatial

import java.util.PriorityQueue

import org.apache.spark.spatialjoin.utils.GeomUtils
import org.locationtech.jts.geom.{Envelope, Point}

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class QuadNode(val env: Envelope) extends GlobalNode {
  private val centre: Point = GeomUtils.getCentre(env)
  private val subnodes = new Array[QuadNode](4)

  def split(samples: Array[Envelope], queue: PriorityQueue[(QuadNode, Array[Envelope])]): Unit = {
    for (index <- 0 until 4) {
      assert(null == subnodes(index))
      subnodes(index) = new QuadNode(createSubEnv(index))
      val subEnv = subnodes(index).env
      val subSamples = for (sample <- samples if subEnv.intersects(sample)) yield sample
      queue.add(subnodes(index), subSamples)
    }
  }

  def findNearestId(point: Point, filter: GlobalNode => Boolean): Int = {
    if (partitionId != -1) {
      return if (filter(this)) partitionId else -1
    }

    val subIndex = if (point.getX > centre.getX) {
      if (point.getY > centre.getY) 3 else 1
    } else {
      if (point.getY > centre.getY) 2 else 0
    }
    var nearestId = subnodes(subIndex).findNearestId(point, filter)
    if (nearestId == -1) {
      for (index <- 0 until 4 if index != subIndex) {
        nearestId = subnodes(index).findNearestId(point, filter)
        if (nearestId != -1) return nearestId
      }
      -1 //not find a partition that has at least k candidates
    } else nearestId
  }

  def findIntersectIds(queryEnv: Envelope, idCollector: ArrayBuffer[Int]): Unit = {
    if (this.env.intersects(queryEnv)) {
      if (this.partitionId != -1) idCollector += partitionId
      else subnodes.foreach(_.findIntersectIds(queryEnv, idCollector))
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
}
package org.apache.spark.spatialjoin

import org.locationtech.jts.geom.{Envelope, Geometry, Point}

import scala.collection.mutable.ArrayBuffer

class QuadNode(val env: Envelope) extends Serializable {
  private val centrex = (env.getMinX + env.getMaxX) / 2
  private val centrey = (env.getMinY + env.getMaxY) / 2
  private val subnodes = new Array[QuadNode](4)
  private var partitionId = -1

  def split(maxNumPerPartition: Int, samples: Array[Envelope],
            partitionCollector: ArrayBuffer[Envelope], level: Int = 0): Unit = {

    //增最大深度防止出现死循环
    if (samples.length < maxNumPerPartition || level >= 15) {
      partitionId = partitionCollector.size
      partitionCollector += this.env
    } else {
      for (index <- 0 until 4) {
        val subEnv = createSubEnv(index)
        val subSamples = for (sample <- samples if subEnv.intersects(sample)) yield sample
        subnodes(index) = new QuadNode(subEnv)
        subnodes(index).split(maxNumPerPartition, subSamples, partitionCollector, level + 1)
      }
    }
  }

  def getIds(geom: Geometry, distance: Double): Array[Int] = {
    val idCollector = new ArrayBuffer[Int]()
    geom match {
      case point: Point if distance == 0.0 =>
        idCollector += placeGeom(point)
      case _ =>
        val geomEnv = geom.getEnvelopeInternal
        if (distance > 0.0) geomEnv.expandBy(distance)
        placeGeom(geomEnv, idCollector)
    }
    idCollector.toArray
  }

  private def placeGeom(geomEnv: Envelope, idCollector: ArrayBuffer[Int]): Unit = {
    if (this.env.intersects(geomEnv)) {
      if (this.partitionId != -1) idCollector += partitionId
      else subnodes.foreach(_.placeGeom(geomEnv, idCollector))
    }
  }

  private def placeGeom(point: Point): Int = {
    if (partitionId != -1) return this.partitionId

    val subnodeIndex = if (point.getX >= centrex) {
      if (point.getY >= centrey) 3 else 1
    } else {
      if (point.getY >= centrey) 2 else 0
    }
    subnodes(subnodeIndex).placeGeom(point)
  }


  private def createSubEnv(index: Int): Envelope = {
    var minx = 0.0
    var maxx = 0.0
    var miny = 0.0
    var maxy = 0.0

    index match {
      case 0 =>
        minx = env.getMinX
        maxx = centrex
        miny = env.getMinY
        maxy = centrey
      case 1 =>
        minx = centrex
        maxx = env.getMaxX
        miny = env.getMinY
        maxy = centrey
      case 2 =>
        minx = env.getMinX
        maxx = centrex
        miny = centrey
        maxy = env.getMaxY
      case 3 =>
        minx = centrex
        maxx = env.getMaxX
        miny = centrey
        maxy = env.getMaxY
    }
    new Envelope(minx, maxx, miny, maxy)
  }
}
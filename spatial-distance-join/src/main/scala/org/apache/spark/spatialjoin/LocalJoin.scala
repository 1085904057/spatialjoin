package org.apache.spark.spatialjoin

import org.locationtech.jts.geom.{Envelope, Geometry, Point}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.operation.distance.DistanceOp

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer

object LocalJoin {
  def join[L, R](leftItems: Iterator[L],
                 rightItems: Iterator[R],
                 leftExtractor: L => Geometry,
                 rightExtractor: R => Geometry,
                 partitionEnv: Envelope,
                 predicate: (Geometry, Geometry) => Boolean,
                 distance: Double): Iterator[(L, R)] = {

    val spatialIndex = new STRtree()
    for (rightItem <- rightItems) {
      spatialIndex.insert(rightExtractor(rightItem).getEnvelopeInternal, rightItem)
    }

    val pairs = new ArrayBuffer[(L, R)]()
    for (leftItem <- leftItems) {
      val leftGeom = leftExtractor(leftItem)
      val leftGeomEnv = leftGeom.getEnvelopeInternal
      if (distance > 0.0) leftGeomEnv.expandBy(distance)
      val candidates = spatialIndex.query(leftGeomEnv).asScala
      for (candidate <- candidates) {
        val rightItem = candidate.asInstanceOf[R]
        val rightGeom = rightExtractor(rightItem)
        if (refine(leftGeom, rightGeom, leftGeomEnv, partitionEnv, predicate, distance)) {
          pairs += ((leftItem, rightItem))
        }
      }
    }
    pairs.toIterator
  }

  def selfJoin[I](items: Iterator[I],
                  extractor: I => Geometry,
                  partitionEnv: Envelope,
                  predicate: (Geometry, Geometry) => Boolean,
                  distance: Double): Iterator[(I, I)] = {

    val itemList = items.toList
    val spatialIndex = new STRtree()
    for (indexItem <- itemList) {
      spatialIndex.insert(extractor(indexItem).getEnvelopeInternal, indexItem)
    }

    val pairs = new ArrayBuffer[(I, I)]()
    for (queryItem <- itemList) {
      val queryGeom = extractor(queryItem)
      val queryGeomEnv = queryGeom.getEnvelopeInternal
      if (distance > 0.0) queryGeomEnv.expandBy(distance)
      val candidates = spatialIndex.query(queryGeomEnv).asScala
      for (candidate <- candidates) {
        val indexItem = candidate.asInstanceOf[I]
        val indexGeom = extractor(indexItem)
        if (refine(queryGeom, indexGeom, queryGeomEnv, partitionEnv, predicate, distance)) {
          pairs += ((queryItem, indexItem))
        }
      }
    }
    pairs.toIterator
  }

  private def refine(leftGeom: Geometry, rightGeom: Geometry, leftGeomEnv: Envelope,
                     partitionEnv: Envelope, predicate: (Geometry, Geometry) => Boolean,
                     distance: Double): Boolean = {

    rightGeom match {
      case point: Point if distance == 0.0 => leftGeom.intersects(point)
      case _ =>
        val intersection = leftGeomEnv.intersection(rightGeom.getEnvelopeInternal)
        if (intersection.getWidth >= 0 &&
          checkReferPoint(partitionEnv, intersection.getMinX, intersection.getMinY)) {

          if (distance > 0.0) {
            return DistanceOp.isWithinDistance(leftGeom, rightGeom, distance)
          } else {
            return predicate(leftGeom, rightGeom)
          }
        }
        false
    }
  }

  private def checkReferPoint(partitionEnv: Envelope, x: Double, y: Double): Boolean = {
    x >= partitionEnv.getMinX &&
      x < partitionEnv.getMaxX &&
      y >= partitionEnv.getMinY &&
      y < partitionEnv.getMaxY
  }
}

package org.apache.spark.spatialjoin.join

import org.apache.spark.spatialjoin.utils.GeomUtils
import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangrubin
 */
trait GeomContainer extends Serializable {
  def getGeom: Geometry
}

class FirstContainer(geom: Geometry, candidates: Array[Geometry]) extends GeomContainer {

  override def getGeom: Geometry = geom

  def getCandidates: Array[Geometry] = candidates
}

class SecondContainer(geomKey: GeomKey, candidatesWithDist: ArrayBuffer[(Geometry, Double)])
  extends GeomContainer {

  override def getGeom: Geometry = geomKey.getGeom

  def getGeomKey: GeomKey = geomKey

  def getCandidatesWithDist: ArrayBuffer[(Geometry, Double)] = candidatesWithDist
}

object GeomContainer {

  def apply(leftGeom: Geometry, candidatesWithDist: ArrayBuffer[(Geometry, Double)],
            partition: (Envelope, Int), k: Int): GeomContainer = {

    val expandDist = candidatesWithDist.last._2
    val expandEnv = leftGeom.getEnvelopeInternal
    expandEnv.expandBy(expandDist)
    val (partitionEnv, partitionId) = partition
    if (partitionEnv.contains(expandEnv)) {
      new FirstContainer(leftGeom, candidatesWithDist.map(_._1).toArray)
    } else {
      val duplicateCandidatesWithDist = candidatesWithDist.filter {
        case (candidate: Geometry, _: Double) =>
          val intersection = expandEnv.intersection(candidate.getEnvelopeInternal)
          GeomUtils.halfIntersect(partitionEnv, intersection.getMinX, intersection.getMinY)
      }

      var safeNum = 0
      val baseEnv = leftGeom.getEnvelopeInternal
      val iter = duplicateCandidatesWithDist.iterator
      while (partitionEnv.contains(baseEnv) && iter.hasNext) {
        baseEnv.expandBy(iter.next()._2)
        safeNum += 1
      }
      val geomKey = GeomKeyWithParam(leftGeom, partitionId, expandDist, k - safeNum)
      new SecondContainer(geomKey, duplicateCandidatesWithDist)
    }
  }
}

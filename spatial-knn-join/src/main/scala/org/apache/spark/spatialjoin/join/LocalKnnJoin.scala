package org.apache.spark.spatialjoin.join

import org.apache.spark.spatialjoin.index.local.STRTree
import org.apache.spark.spatialjoin.utils.GeomUtils
import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangrubin
 */
object LocalKnnJoin {
  def firstRoundJoin(leftGeoms: Iterator[Geometry], localIndex: STRTree,
                     partition: (Envelope, Int), k: Int): Iterator[GeomContainer] = {

    if (leftGeoms.isEmpty || localIndex.isEmpty) return Iterator.empty

    leftGeoms.map(leftGeom => {
      val candidatesWithDist = localIndex.nearestNeighbour(leftGeom, k)
      GeomContainer(leftGeom, candidatesWithDist, partition, k)
    })
  }

  def secondRoundJoin(leftGeoms: Iterator[GeomKeyWithParam],
                      localIndex: STRTree,
                      partitionEnvs: Array[Envelope]): Iterator[SecondContainer] = {

    if (leftGeoms.isEmpty || localIndex.isEmpty) return Iterator.empty

    val containers = new ArrayBuffer[SecondContainer]()
    leftGeoms.foreach {
      case geomKey@GeomKeyWithParam(leftGeom, partitionId, expandDist, shrinkK) =>
        val expandEnv = leftGeom.getEnvelopeInternal
        expandEnv.expandBy(expandDist)
        val isValid = (geomEnv: Envelope) => {
          val intersection = expandEnv.intersection(geomEnv)
          intersection.getWidth >= 0 &&
            GeomUtils.halfIntersect(partitionEnvs(partitionId), intersection.getMinX, intersection.getMinY)
        }
        val candidatesWithDist = localIndex.nearestNeighbour(leftGeom, shrinkK, isValid, expandDist)
        containers += new SecondContainer(new GeomKey(leftGeom).bindId(geomKey.getId), candidatesWithDist)
    }
    containers.toIterator
  }
}

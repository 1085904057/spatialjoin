package org.apache.spark.spatialjoin.index.global.spatial

import java.sql.Timestamp

import org.apache.spark.spatialjoin.index.local.time.TRCBasedBins
import org.locationtech.jts.geom.{Envelope, Geometry, Point}

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
trait GlobalIndex extends Serializable {

  def findNearestId(queryCentre: Point, timeFilter: GlobalNode => Boolean): Int

  def findIntersectIds(queryEnv: Envelope, idCollector: ArrayBuffer[Int]): Unit

  def assignPartitionId(baseId: Int): Int

  def getLeafEnv(index: Int): Envelope

  def updateBound(leafNodeMap: Map[Int, Envelope]): Unit = {}

  def getPartitionId(queryGeom: Geometry,
                     queryRange: (Timestamp, Timestamp),
                     timeBinMap: Map[Int, TRCBasedBins]): Int = {

    val filter = (leafNode: GlobalNode) => {
      val timeBin = timeBinMap.get(leafNode.getPartitionId)
      if (timeBin.isEmpty) false else timeBin.get.hasKnn(queryRange)
    }
    getPartitionId(queryGeom, filter)
  }

  def getPartitionId(queryGeom: Geometry,
                     filter: GlobalNode => Boolean = _ => true): Int = {
    val queryCentre = queryGeom.getFactory.createPoint(queryGeom.getEnvelopeInternal.centre())
    findNearestId(queryCentre, filter)
  }

  def getPartitionIds(queryGeom: Geometry, distance: Double = 0.0): Array[Int] = {
    val idCollector = new ArrayBuffer[Int]()
    val geomEnv = queryGeom.getEnvelopeInternal
    if (distance > 0.0) geomEnv.expandBy(distance)
    findIntersectIds(geomEnv, idCollector)
    idCollector.toArray
  }
}

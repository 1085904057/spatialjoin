package org.apache.spark.spatialjoin.index.global

import org.apache.spark.spatialjoin.utils.GeomUtils
import org.locationtech.jts.geom.{Envelope, Geometry, Point}

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangrubin
 */
abstract class IndexNode(val env: Envelope) extends Serializable {
  /**
    * partition id
    **/
  protected var partitionId: Int = -1

  /**
    * number of samples in current partition
    **/
  protected var num: Int = -1

  def setNum(num: Int): Unit = this.num = num

  /**
    * split root index node by samples and collect all of leaf node's env and partitions
    *
    * @param maxNumPerPartition max number of samples per partition contains
    * @param samples            samples to split
    * @return leaf nodes' env
    **/
  def split(maxNumPerPartition: Int, samples: Array[Envelope], k: Int = 0): Array[IndexNode]

  /**
    * bind the id of partitions which intersect with the geom's expanded mbr
    *
    * @param geom     geom
    * @param distance distance to expand
    * @return partition ids
    **/
  def bindIds(geom: Geometry, distance: Double = 0.0): Array[Int] = {
    val idCollector = new ArrayBuffer[Int]()
    geom match {
      case point: Point if distance == 0.0 =>
        idCollector += getId(point)
      case _ =>
        val geomEnv = geom.getEnvelopeInternal
        if (distance > 0.0) geomEnv.expandBy(distance)
        collectIds(geomEnv, idCollector)
    }
    idCollector.toArray
  }

  /**
    * bind partition id for the geom when execute first round knn partition
    *
    * @param geom geom
    * @param k    k value
    * @return id of the nearest partition to geom
    **/
  def bindId(geom: Geometry, k: Int): Int = {
    val geomCentre = GeomUtils.getCentre(geom.getEnvelopeInternal)
    getNearestId(geomCentre, k)
  }

  /**
    * get partition id which contains point
    *
    * @param point point
    * @return partition id
    **/
  protected def getId(point: Point): Int

  /**
    * get nearest partition's id
    *
    * @param point point
    * @param k     k value
    * @return partition id
    **/
  protected def getNearestId(point: Point, k: Int): Int

  /**
    * collect id of partitions which intersect bound
    *
    * @param bound       geometry mbr or expanded mbr
    * @param idCollector partition id collector
    **/
  protected def collectIds(bound: Envelope, idCollector: ArrayBuffer[Int]): Unit
}

object IndexNode {
  def apply(globalEnv: Envelope): IndexNode = new QuadNode(globalEnv)
}

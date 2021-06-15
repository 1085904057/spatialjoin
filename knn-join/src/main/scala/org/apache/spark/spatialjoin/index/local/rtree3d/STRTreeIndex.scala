package org.apache.spark.spatialjoin.index.local.rtree3d

import java.sql.Timestamp

import org.apache.spark.spatialjoin.extractor.STExtractor
import org.apache.spark.spatialjoin.index.local.rtree.{Boundable, RTreeIndex, RTreeNode}
import org.apache.spark.spatialjoin.utils.TimeUtils
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class STRTreeIndex[T](extractor: STExtractor[T], override val nodeCapacity: Int = 10)
  extends RTreeIndex[T](extractor, nodeCapacity) {

  override def insert(item: T): Unit = {
    empty = false
    itemBoundables += new STItemBoundable(item, extractor)
  }

  override protected def createNode(level: Int): RTreeNode = new STRTreeNode(level)

  override protected def createParentBoundables(childBoundables: ArrayBuffer[Boundable],
                                                newLevel: Int): ArrayBuffer[Boundable] = {
    //prepare parameters
    assert(childBoundables.nonEmpty)
    val minLeafCount = Math.ceil(childBoundables.length / nodeCapacity.toDouble).toInt
    val sliceCount = Math.ceil(Math.pow(minLeafCount, 1 / 3)).toInt

    //split horizontally
    val timeSortIter = childBoundables.sortBy(_.asInstanceOf[STBoundable].centreT)(TimeUtils.timeOrdering).iterator
    val sliceCapacity = Math.ceil(childBoundables.size / sliceCount.toDouble).toInt
    val parentBoundables = new ArrayBuffer[Boundable]
    for (_ <- 0 until sliceCount) {
      val timeSlice = new ArrayBuffer[Boundable](sliceCapacity)
      while (timeSortIter.hasNext && timeSlice.length < sliceCapacity) {
        timeSlice += timeSortIter.next
      }
      val verticalSlices = super.createVerticalSlices(timeSlice, sliceCount)
      for (i <- verticalSlices.indices) {
        parentBoundables ++= super.createParentBoundableFromVerticalSlice(verticalSlices(i), newLevel)
      }
    }

    //result
    parentBoundables
  }

  def nearestNeighbour(queryGeom: Geometry,
                       queryStart: Timestamp,
                       queryEnd: Timestamp,
                       k: Int,
                       isValid: T => Boolean,
                       maxDistance: Double): Array[(T, Double)] = {

    //build index
    if (!built) build()

    val queryExtractor = new STExtractor[(Geometry, Timestamp, Timestamp)] {
      override def startTime(row: (Geometry, Timestamp, Timestamp)): Timestamp = queryStart

      override def endTime(row: (Geometry, Timestamp, Timestamp)): Timestamp = queryEnd

      override def geom(row: (Geometry, Timestamp, Timestamp)): Geometry = queryGeom
    }

    var distanceLowerBound = maxDistance
    // initialize node queue
    val nodeQueue = buildQueue(true)
    val queryItem = (queryGeom, queryStart, queryEnd)
    val queryBoundable = new STItemBoundable[(Geometry, Timestamp, Timestamp)](queryItem, queryExtractor)
    nodeQueue.add((root, distance(root, queryBoundable)))

    //calculate k nearest neighbors
    val candidateQueue = buildQueue(false)
    while (!nodeQueue.isEmpty && nodeQueue.peek()._2 < distanceLowerBound) {
      val (nearestNode, currentDistance) = nodeQueue.poll
      nearestNode match {
        case leafNode: STItemBoundable[T] =>
          if (isValid(leafNode.getItem)) {
            if (candidateQueue.size < k) {
              candidateQueue.add((leafNode, currentDistance))
            } else {
              if (candidateQueue.peek._2 > currentDistance) {
                candidateQueue.poll
                candidateQueue.add((leafNode, currentDistance))
              }
              distanceLowerBound = candidateQueue.peek._2
            }
          }
        case noLeafNode: RTreeNode =>
          noLeafNode.getChildBoundables.foreach(childBoundable => {
            val child = childBoundable.asInstanceOf[STBoundable]
            if (TimeUtils.isIntersects((queryStart, queryEnd), (child.getMinTime, child.getMaxTime))) {
              val dist = distance(child, queryBoundable)
              nodeQueue.add((child, dist))
            }
          })
      }
    }

    //return result
    candidateQueue.toArray(new Array[(STBoundable, Double)](0)).map {
      case (candidate, distance) => (candidate.asInstanceOf[STItemBoundable[T]].getItem, distance)
    }.sortBy(_._2)
  }
}
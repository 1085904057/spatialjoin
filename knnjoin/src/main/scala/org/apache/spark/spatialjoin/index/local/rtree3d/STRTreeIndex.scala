package org.apache.spark.spatialjoin.index.local.rtree3d

import java.sql.Timestamp
import java.util.PriorityQueue

import org.apache.spark.spatialjoin.extractor.STExtractor
import org.apache.spark.spatialjoin.utils.TimeUtils
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class STRTreeIndex[T](extractor: STExtractor[T], val nodeCapacity: Int = 10) extends Serializable {
  protected var root: STRTreeNode = _
  protected var itemBoundables = new ArrayBuffer[STItemBoundable[T]]()
  protected var empty: Boolean = true
  protected var built = false

  def insert(item: T): Unit = {
    empty = false
    itemBoundables += new STItemBoundable(item, extractor)
  }

  def build(): Unit = this.synchronized {
    if (!this.built) {
      this.root = if (this.itemBoundables.isEmpty) this.createNode(0)
      else this.createHigherLevels(this.itemBoundables.map(_.asInstanceOf[STBoundable]), -1)
      this.itemBoundables = null
      this.built = true
    }
  }

  def isEmpty: Boolean = empty

  private def createNode(level: Int): STRTreeNode = new STRTreeNode(level)

  private def createHigherLevels(boundables: ArrayBuffer[STBoundable], level: Int): STRTreeNode = {
    assert(boundables.nonEmpty)
    val parentBoundables = this.createParentBoundables(boundables, level + 1)
    if (parentBoundables.length == 1) parentBoundables(0).asInstanceOf[STRTreeNode]
    else this.createHigherLevels(parentBoundables, level + 1)
  }

  protected def createParentBoundables(childBoundables: ArrayBuffer[STBoundable],
                                       newLevel: Int): ArrayBuffer[STBoundable] = {
    //prepare parameters
    assert(childBoundables.nonEmpty)
    val minLeafCount = Math.ceil(childBoundables.length / nodeCapacity.toDouble).toInt
    val sliceCount = Math.ceil(Math.pow(minLeafCount, 1 / 3)).toInt

    //split horizontally
    val timeSortIter = childBoundables.sortBy(_.centreT)(TimeUtils.timeOrdering).iterator
    val sliceCapacity = Math.ceil(childBoundables.size / sliceCount.toDouble).toInt
    val parentBoundables = new ArrayBuffer[STBoundable]
    for (_ <- 0 until sliceCount) {
      val timeSlice = new ArrayBuffer[STBoundable](sliceCapacity)
      while (timeSortIter.hasNext && timeSlice.length < sliceCapacity) {
        timeSlice += timeSortIter.next
      }
      val verticalSlices = createVerticalSlices(timeSlice, sliceCount)
      for (i <- verticalSlices.indices) {
        parentBoundables ++= createParentBoundableFromVerticalSlice(verticalSlices(i), newLevel)
      }
    }

    //result
    parentBoundables
  }

  //horizontal split
  private def createVerticalSlices(childBoundables: ArrayBuffer[STBoundable],
                                   sliceCount: Int): Array[ArrayBuffer[STBoundable]] = {

    val sliceCapacity = Math.ceil(childBoundables.size / sliceCount.toDouble).toInt
    val iter = childBoundables.sortBy(_.centreX).iterator
    val slices = for (_ <- 0 until sliceCount) yield {
      val currentSlice = new ArrayBuffer[STBoundable](sliceCapacity)
      while (iter.hasNext && currentSlice.length < sliceCapacity) {
        currentSlice += iter.next
      }
      currentSlice
    }
    slices.filter(_.nonEmpty).toArray
  }

  //vertical split
  protected def createParentBoundableFromVerticalSlice(childBoundables: ArrayBuffer[STBoundable],
                                                       newLevel: Int): ArrayBuffer[STBoundable] = {

    assert(childBoundables.nonEmpty)
    val parentBoundables = new ArrayBuffer[STRTreeNode]
    parentBoundables += this.createNode(newLevel)

    val sortedChildBoundables = childBoundables.sortBy(_.centreY)
    val iter = sortedChildBoundables.iterator
    while (iter.hasNext) {
      if (parentBoundables.last.size == nodeCapacity)
        parentBoundables += this.createNode(newLevel)
      parentBoundables.last.addChildBoundable(iter.next)
    }

    parentBoundables.map(_.asInstanceOf[STBoundable])
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
        case noLeafNode: STRTreeNode =>
          noLeafNode.getChildBoundables.foreach(childBoundable => {
            val child = childBoundable
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

  private def distance(oneBoundable: STBoundable, otherBoundable: STBoundable): Double = {
    (oneBoundable, otherBoundable) match {
      case (oneItemBoundable: STItemBoundable[T], otherItemBoundable: STItemBoundable[_]) =>
        oneItemBoundable.getGeom.distance(otherItemBoundable.getGeom)
      case _ =>
        oneBoundable.getBound.distance(otherBoundable.getBound)
    }
  }

  private def buildQueue(isNormal: Boolean): PriorityQueue[(STBoundable, Double)] = {
    val comparator = new Ordering[(STBoundable, Double)] {
      override def compare(x: (STBoundable, Double), y: (STBoundable, Double)): Int = {
        val value = java.lang.Double.compare(x._2, y._2)
        if (isNormal) value else -value
      }
    }
    new PriorityQueue[(STBoundable, Double)](comparator)
  }
}
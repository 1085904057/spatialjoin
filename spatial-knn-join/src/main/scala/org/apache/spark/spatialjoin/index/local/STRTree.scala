package org.apache.spark.spatialjoin.index.local

import java.util.PriorityQueue

import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangrubin
 */
class STRTree(nodeCapacity: Int = 10) extends Serializable {
  protected var root: STRtreeNode = _
  private var itemBoundables = new ArrayBuffer[ItemBoundable]()
  private var size = 0
  private var built = false

  def insert(itemEnv: Envelope, item: Geometry): Unit = {
    size += 1
    itemBoundables += new ItemBoundable(itemEnv, item)
  }

  def build(): Unit = this.synchronized {
    if (!this.built) {
      this.root = if (this.itemBoundables.isEmpty) this.createNode(0)
      else this.createHigherLevels(this.itemBoundables.map(_.asInstanceOf[Boundable]), -1)
      this.itemBoundables = null
      this.built = true
    }
  }

  def isEmpty: Boolean = size == 0

  private def createNode(level: Int): STRtreeNode = new STRtreeNode(level)

  private def createHigherLevels(boundables: ArrayBuffer[Boundable], level: Int): STRtreeNode = {
    assert(boundables.nonEmpty)
    val parentBoundables = this.createParentBoundables(boundables, level + 1)
    if (parentBoundables.length == 1) parentBoundables(0).asInstanceOf[STRtreeNode]
    else this.createHigherLevels(parentBoundables, level + 1)
  }

  private def createParentBoundables(childBoundables: ArrayBuffer[Boundable],
                                     newLevel: Int): ArrayBuffer[Boundable] = {
    //prepare parameters
    assert(childBoundables.nonEmpty)
    val minLeafCount = Math.ceil(childBoundables.length / nodeCapacity.toDouble).toInt
    val sliceCount = Math.ceil(Math.sqrt(minLeafCount)).toInt

    //split horizontally
    val sortedChildBoundables = childBoundables.sortBy(_.centreX)
    val verticalSlices = createVerticalSlices(sortedChildBoundables, sliceCount)

    //split vertically
    val parentBoundables = new ArrayBuffer[Boundable]
    for (i <- verticalSlices.indices) {
      parentBoundables ++= createParentBoundableFromVerticalSlice(verticalSlices(i), newLevel)
    }

    //result
    parentBoundables
  }

  //vertical split
  private def createParentBoundableFromVerticalSlice(childBoundables: ArrayBuffer[Boundable],
                                                     newLevel: Int): ArrayBuffer[Boundable] = {

    assert(childBoundables.nonEmpty)
    val parentBoundables = new ArrayBuffer[STRtreeNode]
    parentBoundables += this.createNode(newLevel)

    val sortedChildBoundables = childBoundables.sortBy(_.centreY)
    val iter = sortedChildBoundables.iterator
    while (iter.hasNext) {
      if (parentBoundables.last.size == nodeCapacity)
        parentBoundables += this.createNode(newLevel)
      parentBoundables.last.addChildBoundable(iter.next)
    }

    parentBoundables.map(_.asInstanceOf[Boundable])
  }

  //horizontal split
  private def createVerticalSlices(childBoundables: ArrayBuffer[Boundable],
                                   sliceCount: Int): Array[ArrayBuffer[Boundable]] = {

    val sliceCapacity = Math.ceil(childBoundables.size / sliceCount.toDouble).toInt
    val iter = childBoundables.iterator
    val slices = for (_ <- 0 until sliceCount) yield {
      val currentSlice = new ArrayBuffer[Boundable](sliceCapacity)
      while (iter.hasNext && currentSlice.length < sliceCapacity) {
        currentSlice += iter.next
      }
      currentSlice
    }
    slices.toArray
  }

  def nearestNeighbour(queryGeom: Geometry, k: Int,
                       isValid: Envelope => Boolean = (_: Envelope) => true,
                       maxDistance: Double = Double.MaxValue): ArrayBuffer[(Geometry, Double)] = {

    //build index
    if (!built) build()

    var distanceLowerBound = maxDistance
    // initialize node queue
    val nodeQueue = buildQueue(true)
    val queryBoundable = new ItemBoundable(queryGeom.getEnvelopeInternal, queryGeom)
    nodeQueue.add((root, distance(root, queryBoundable)))

    //calculate k nearest neighbors
    val candidateQueue = buildQueue(false)
    while (!nodeQueue.isEmpty && nodeQueue.peek()._2 < distanceLowerBound) {
      val (nearestNode, currentDistance) = nodeQueue.poll
      nearestNode match {
        case leafNode: ItemBoundable =>
          if (isValid(leafNode.getBound)) {
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
        case noLeafNode: STRtreeNode =>
          noLeafNode.getChildBoundables.foreach(childBoundable => {
            val dist =  distance(childBoundable, queryBoundable)
            nodeQueue.add((childBoundable,dist))
          })
      }
    }

    //return result
    candidateQueue.toArray(new Array[(Boundable, Double)](0)).map {
      case (candidate, distance) => (candidate.asInstanceOf[ItemBoundable].getItem, distance)
    }.toBuffer.asInstanceOf[ArrayBuffer[(Geometry, Double)]].sortBy(_._2)
  }

  private def distance(oneBoundable: Boundable, otherBoundable: Boundable): Double = {
    (oneBoundable, otherBoundable) match {
      case (oneItemBoundable: ItemBoundable, otherItemBoundable: ItemBoundable) =>
        oneItemBoundable.getItem.distance(otherItemBoundable.getItem)
      case _ =>
        oneBoundable.getBound.distance(otherBoundable.getBound)
    }
  }

  private def buildQueue(isNormal: Boolean): PriorityQueue[(Boundable, Double)] = {
    val comparator = new Ordering[(Boundable, Double)] {
      override def compare(x: (Boundable, Double), y: (Boundable, Double)): Int = {
        val value = java.lang.Double.compare(x._2, y._2)
        if (isNormal) value else -value
      }
    }
    new PriorityQueue[(Boundable, Double)](comparator)
  }
}
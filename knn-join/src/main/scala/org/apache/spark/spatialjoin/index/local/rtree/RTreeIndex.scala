package org.apache.spark.spatialjoin.index.local.rtree

import java.util.PriorityQueue

import org.apache.spark.spatialjoin.extractor.GeomExtractor

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class RTreeIndex[T](extractor: GeomExtractor[T], val nodeCapacity: Int = 10) extends Serializable {
  protected var root: RTreeNode = _
  protected var itemBoundables = new ArrayBuffer[ItemBoundable[T]]()
  protected var empty: Boolean = true
  protected var built = false

  def insert(item: T): Unit = {
    empty = false
    itemBoundables += new ItemBoundable(item, extractor)
  }

  def build(): Unit = this.synchronized {
    if (!this.built) {
      this.root = if (this.itemBoundables.isEmpty) this.createNode(0)
      else this.createHigherLevels(this.itemBoundables.map(_.asInstanceOf[Boundable]), -1)
      this.itemBoundables = null
      this.built = true
    }
  }

  def isEmpty: Boolean = empty

  protected def createNode(level: Int): RTreeNode = new RTreeNode(level)

  private def createHigherLevels(boundables: ArrayBuffer[Boundable], level: Int): RTreeNode = {
    assert(boundables.nonEmpty)
    val parentBoundables = this.createParentBoundables(boundables, level + 1)
    if (parentBoundables.length == 1) parentBoundables(0).asInstanceOf[RTreeNode]
    else this.createHigherLevels(parentBoundables, level + 1)
  }

  protected def createParentBoundables(childBoundables: ArrayBuffer[Boundable],
                                       newLevel: Int): ArrayBuffer[Boundable] = {
    //prepare parameters
    assert(childBoundables.nonEmpty)
    val minLeafCount = Math.ceil(childBoundables.length / nodeCapacity.toDouble).toInt
    val sliceCount = Math.ceil(Math.sqrt(minLeafCount)).toInt

    //split horizontally
    val verticalSlices = createVerticalSlices(childBoundables, sliceCount)

    //split vertically
    val parentBoundables = new ArrayBuffer[Boundable]
    for (i <- verticalSlices.indices) {
      parentBoundables ++= createParentBoundableFromVerticalSlice(verticalSlices(i), newLevel)
    }

    //result
    parentBoundables
  }

  //horizontal split
  protected def createVerticalSlices(childBoundables: ArrayBuffer[Boundable],
                                     sliceCount: Int): Array[ArrayBuffer[Boundable]] = {

    val sliceCapacity = Math.ceil(childBoundables.size / sliceCount.toDouble).toInt
    val iter = childBoundables.sortBy(_.centreX).iterator
    val slices = for (_ <- 0 until sliceCount) yield {
      val currentSlice = new ArrayBuffer[Boundable](sliceCapacity)
      while (iter.hasNext && currentSlice.length < sliceCapacity) {
        currentSlice += iter.next
      }
      currentSlice
    }
    slices.filter(_.nonEmpty).toArray
  }

  //vertical split
  protected def createParentBoundableFromVerticalSlice(childBoundables: ArrayBuffer[Boundable],
                                                       newLevel: Int): ArrayBuffer[Boundable] = {

    assert(childBoundables.nonEmpty)
    val parentBoundables = new ArrayBuffer[RTreeNode]
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

  protected def distance(oneBoundable: Boundable, otherBoundable: Boundable): Double = {
    (oneBoundable, otherBoundable) match {
      case (oneItemBoundable: ItemBoundable[T], otherItemBoundable: ItemBoundable[_]) =>
        oneItemBoundable.getGeom.distance(otherItemBoundable.getGeom)
      case _ =>
        oneBoundable.getBound.distance(otherBoundable.getBound)
    }
  }

  protected def buildQueue(isNormal: Boolean): PriorityQueue[(Boundable, Double)] = {
    val comparator = new Ordering[(Boundable, Double)] {
      override def compare(x: (Boundable, Double), y: (Boundable, Double)): Int = {
        val value = java.lang.Double.compare(x._2, y._2)
        if (isNormal) value else -value
      }
    }
    new PriorityQueue[(Boundable, Double)](comparator)
  }
}
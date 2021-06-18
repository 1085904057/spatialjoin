package org.apache.spark.spatialjoin.index.global.spatial

import java.util.PriorityQueue

import org.locationtech.jts.geom.{Envelope, Point}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class GlobalRTree(nodeCapacity: Int) extends GlobalIndex {
  private var root: RTreeNode = _
  private var leafNodes: ArrayBuffer[RTreeNode] = _

  def build(samples: Array[Envelope]): Unit = {
    require(samples.nonEmpty, "samples must be not empty")
    this.leafNodes = createParentBoundables(samples.toBuffer.asInstanceOf[ArrayBuffer[Envelope]], 0)
    this.root = this.createHigherLevels(this.leafNodes, 1)
  }

  private def createNode(childBoundables: Array[_ <: Envelope], level: Int): RTreeNode = {
    val bound = new Envelope()
    childBoundables.foreach(bound.expandToInclude)
    val node = new RTreeNode(bound, level)
    if (level > 0) {
      node.addChildNodes(childBoundables.map(_.asInstanceOf[RTreeNode]))
    }
    node
  }

  private def createHigherLevels(boundables: ArrayBuffer[RTreeNode], higherLevel: Int): RTreeNode = {
    assert(boundables.nonEmpty)
    val parentBoundables = this.createParentBoundables(boundables, higherLevel)
    if (parentBoundables.length == 1) parentBoundables(0)
    else this.createHigherLevels(parentBoundables, higherLevel + 1)
  }

  private def createParentBoundables(childBoundables: ArrayBuffer[_ <: Envelope],
                                     parentLevel: Int): ArrayBuffer[RTreeNode] = {
    //prepare parameters
    assert(childBoundables.nonEmpty)
    val minLeafCount = Math.ceil(childBoundables.length / nodeCapacity.toDouble).toInt
    val sliceCount = Math.ceil(Math.sqrt(minLeafCount)).toInt

    //split horizontally
    val verticalSlices = createVerticalSlices(childBoundables, sliceCount)

    //split vertically
    val parentBoundables = new ArrayBuffer[RTreeNode]
    for (i <- verticalSlices.indices) {
      parentBoundables ++= createParentBoundableFromVerticalSlice(verticalSlices(i), parentLevel)
    }

    //result
    parentBoundables
  }

  //horizontal split
  private def createVerticalSlices(childBoundables: ArrayBuffer[_ <: Envelope],
                                   sliceCount: Int): Array[ArrayBuffer[_ <: Envelope]] = {

    val sliceCapacity = Math.ceil(childBoundables.size / sliceCount.toDouble).toInt
    childBoundables.sortBy(bound => bound.getMinX + bound.getMaxX) //sorted by centre x
      .grouped(sliceCapacity).filter(_.nonEmpty).toArray
  }

  //vertical split
  private def createParentBoundableFromVerticalSlice(childBoundables: ArrayBuffer[_ <: Envelope],
                                                     parentLevel: Int): Array[RTreeNode] = {

    assert(childBoundables.nonEmpty)
    childBoundables.sortBy(bound => bound.getMinY + bound.getMaxY) //sort by centre y
      .grouped(nodeCapacity).map(boundables => {
      this.createNode(boundables.toArray, parentLevel)
    }).toArray
  }

  override def updateBound(leafNodeMap: Map[Int, Envelope]): Unit = {
    root.updateNodeBound(leafNodeMap)
  }

  private def distance(oneBoundable: RTreeNode, otherBoundable: Point): Double = {
    oneBoundable.centre().distance(otherBoundable.getCoordinate)
  }

  override def findNearestId(queryCentre: Point,
                             timeFilter: GlobalNode => Boolean): Int = {

    val nodeQueue = buildQueue(true)
    nodeQueue.add((root, distance(root, queryCentre)))

    while (!nodeQueue.isEmpty) {
      val (nearestNode, _) = nodeQueue.poll
      nearestNode match {
        case node: RTreeNode =>
          if (node.getLevel > 0) { //no leaf node
            node.getChildNodes.foreach(child => {
              nodeQueue.add(child, distance(child, queryCentre))
            })
          } else { //leaf node
            if (timeFilter(node)) return node.getPartitionId
          }
      }
    }
    -1 //not find a partition that has at least k candidates
  }

  override def findIntersectIds(queryEnv: Envelope,
                                idCollector: ArrayBuffer[Int]): Unit = {

    val queue = new mutable.Queue[RTreeNode]()
    if (queryEnv.intersects(root.bound)) queue.enqueue(root)
    while (queue.nonEmpty) {
      queue.dequeue() match {
        case node: RTreeNode =>
          if (node.getLevel > 0) { //no leaf node
            queue.enqueue(node.getChildNodes.filter(_.bound.intersects(queryEnv)): _*)
          } else { //leaf node
            idCollector += node.getPartitionId
          }
      }
    }
  }

  override def assignPartitionId(baseId: Int): Int = {
    for (i <- leafNodes.indices) {
      leafNodes(i).setPartitionId(baseId + i)
    }
    baseId + leafNodes.length
  }

  override def getLeafEnv(index: Int): Envelope = leafNodes(index).bound

  private def buildQueue(isNormal: Boolean): PriorityQueue[(RTreeNode, Double)] = {
    val comparator = new Ordering[(RTreeNode, Double)] {
      override def compare(x: (RTreeNode, Double), y: (RTreeNode, Double)): Int = {
        val value = java.lang.Double.compare(x._2, y._2)
        if (isNormal) value else -value
      }
    }
    new PriorityQueue[(RTreeNode, Double)](comparator)
  }
}

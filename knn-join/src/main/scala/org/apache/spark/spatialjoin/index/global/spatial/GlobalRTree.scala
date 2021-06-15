package org.apache.spark.spatialjoin.index.global.spatial

import org.apache.spark.spatialjoin.extractor.GeomExtractor
import org.apache.spark.spatialjoin.index.local.rtree.{Boundable, ItemBoundable, RTreeIndex, RTreeNode}
import org.apache.spark.spatialjoin.utils.GeomUtils
import org.locationtech.jts.geom.{Envelope, Geometry, Point}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class GlobalRTree(nodeCapacity: Int) extends RTreeIndex[Envelope](
  new GeomExtractor[Envelope] {
    override def geom(row: Envelope): Geometry = GeomUtils.defaultFactory.toGeometry(row)
  }, nodeCapacity) with GlobalIndex {

  private var leafNodes: Array[RTreeNode] = _

  def build(samples: Array[Envelope]): Unit = {
    samples.foreach(env => insert(env))
    build() //build rtree index
    val leafNodeBuffer = new ArrayBuffer[RTreeNode]()
    root.collectLeafNodes(leafNodeBuffer)
    leafNodes = leafNodeBuffer.toArray
  }

  override def findNearestId(queryCentre: Point,
                             timeFilter: GlobalNode => Boolean): Int = {

    val nodeQueue = buildQueue(true)
    val queryExtractor = new GeomExtractor[Point] {
      override def geom(row: Point): Geometry = row
    }
    val queryBoundable = new ItemBoundable(queryCentre, queryExtractor)
    nodeQueue.add((root, distance(root, queryBoundable)))

    while (!nodeQueue.isEmpty) {
      val (nearestNode, _) = nodeQueue.poll
      nearestNode match {
        case node: RTreeNode =>
          if (node.getLevel > 0) { //no leaf node
            node.getChildBoundables.foreach(child => {
              nodeQueue.add(child, distance(child, queryBoundable))
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

    val queue = new mutable.Queue[Boundable]()
    if (queryEnv.intersects(root.getBound)) queue.enqueue(root)
    while (queue.nonEmpty) {
      queue.dequeue() match {
        case node: RTreeNode =>
          if (node.getLevel > 0) { //no leaf node
            queue.enqueue(node.getChildBoundables.filter(_.getBound.intersects(queryEnv)): _*)
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

  override def getLeafEnv(index: Int): Envelope = leafNodes(index).getBound

  override def distance(oneBoundable: Boundable, otherBoundable: Boundable): Double = {
    (oneBoundable, otherBoundable) match {
      case (oneItemBoundable: ItemBoundable[Envelope], otherItemBoundable: ItemBoundable[Point]) =>
        oneItemBoundable.getItem.centre().distance(otherItemBoundable.getItem.getCoordinate)
      case _ =>
        oneBoundable.getBound.distance(otherBoundable.getBound)
    }
  }

  override def updateBound(leafNodeMap: Map[Int, Envelope]): Unit = {
    root.updateNodeBound(leafNodeMap)
  }
}

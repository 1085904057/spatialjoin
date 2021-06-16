package org.apache.spark.spatialjoin.index.local.rtree

import org.apache.spark.spatialjoin.index.global.spatial.GlobalNode
import org.locationtech.jts.geom.Envelope

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class RTreeNode(level: Int) extends Boundable with GlobalNode {
  protected var childBoundables = new ArrayBuffer[Boundable]()
  private var bound: Envelope = _

  override def getBound: Envelope = {
    if (this.bound == null) {
      this.bound = new Envelope()
      assert(childBoundables.nonEmpty)
      for (subNode <- childBoundables) {
        this.bound.expandToInclude(subNode.getBound)
      }
    }
    this.bound
  }

  def getLevel: Int = this.level

  def size: Int = this.childBoundables.size

  def isEmpty: Boolean = this.childBoundables.isEmpty

  def addChildBoundable(childNode: Boundable): Unit = {
    assert(this.bound == null)
    this.childBoundables += childNode
  }

  def getChildBoundables: Array[Boundable] = {
    this.childBoundables.toArray
  }

  //collect leaf nodes and set items null
  def collectLeafNodes(partitionCollector: ArrayBuffer[RTreeNode]): Unit = {
    if (this.level > 0) {
      getChildBoundables.foreach {
        case childNode: RTreeNode =>
          childNode.collectLeafNodes(partitionCollector)
      }
      getBound
    } else {
      partitionCollector += this
      getBound
      this.childBoundables = null
    }
  }

  //update partition bound after data assignment for dataset S
  def updateNodeBound(leafBound: Map[Int, Envelope]): Envelope = {
    assert(bound != null)
    if (this.level > 0) { // no leaf node
      childBoundables.foreach {
        case node: RTreeNode =>
          bound.expandToInclude(node.updateNodeBound(leafBound))
      }
    } else { //leaf node
      val boundOpt = leafBound.get(partitionId)
      if (boundOpt.isDefined) bound = boundOpt.get
    }
    bound
  }
}

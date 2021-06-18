package org.apache.spark.spatialjoin.index.global.spatial

import org.locationtech.jts.geom.Envelope

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class RTreeNode(var bound: Envelope, val level: Int) extends Envelope(bound) with GlobalNode {
  protected val childNodes = new ArrayBuffer[RTreeNode]()

  def getLevel: Int = this.level

  def size: Int = this.childNodes.size

  def isEmpty: Boolean = this.childNodes.isEmpty

  def addChildNodes(childNodes: Array[RTreeNode]): Unit = {
    this.childNodes ++= childNodes
  }

  def getChildNodes: Array[RTreeNode] = this.childNodes.toArray

  //update partition bound after data assignment for dataset S
  def updateNodeBound(leafBound: Map[Int, Envelope]): Envelope = {
    if (this.level > 0) { // no leaf node
      childNodes.foreach {
        node: RTreeNode =>
          bound.expandToInclude(node.updateNodeBound(leafBound))
      }
    } else { //leaf node
      val boundOpt = leafBound.get(partitionId)
      if (boundOpt.isDefined) bound = boundOpt.get
    }
    bound
  }
}
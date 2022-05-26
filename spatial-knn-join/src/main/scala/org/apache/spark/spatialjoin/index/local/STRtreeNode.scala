package org.apache.spark.spatialjoin.index.local

import org.locationtech.jts.geom.Envelope

import scala.collection.mutable.ArrayBuffer

/**
 * @author wangrubin
 */
class STRtreeNode(level: Int) extends Boundable with Serializable {
  private val childBoundables = new ArrayBuffer[Boundable]()
  private var bound: Envelope = _

  def getBound: Envelope = {
    if (this.bound == null) {
      this.bound = new Envelope()
      assert(childBoundables.nonEmpty) //todo: remove
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
    assert(this.bound == null) //todo: remove
    this.childBoundables += childNode
  }

  def getChildBoundables: Array[Boundable] = {
    this.childBoundables.toArray
  }
}

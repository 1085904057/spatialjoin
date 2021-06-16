package org.apache.spark.spatialjoin.index.local.rtree3d

import java.sql.Timestamp

import org.apache.spark.spatialjoin.utils.TimeUtils
import org.locationtech.jts.geom.Envelope

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class STRTreeNode(level: Int) extends STBoundable {
  protected var childBoundables = new ArrayBuffer[STBoundable]()
  private var bound: Envelope = _
  private var minTime: Timestamp = _
  private var maxTime: Timestamp = _

  override def getMinTime: Timestamp = {
    if (minTime == null) {
      minTime = childBoundables.map(_.getMinTime).min(TimeUtils.timeOrdering)
    }
    minTime
  }

  override def getMaxTime: Timestamp = {
    if (maxTime == null) {
      maxTime = childBoundables.map(_.getMaxTime).max(TimeUtils.timeOrdering)
    }
    maxTime
  }

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

  def addChildBoundable(childNode: STBoundable): Unit = {
    assert(this.bound == null)
    this.childBoundables += childNode
  }

  def getChildBoundables: Array[STBoundable] = {
    this.childBoundables.toArray
  }
}

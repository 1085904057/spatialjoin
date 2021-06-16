package org.apache.spark.spatialjoin.index.local.rtree3d

import java.sql.Timestamp

import org.apache.spark.spatialjoin.index.local.rtree.RTreeNode
import org.apache.spark.spatialjoin.utils.TimeUtils

/**
  * @author wangrubin3
  **/
class STRTreeNode(level: Int) extends RTreeNode(level) with STBoundable {
  private var minTime: Timestamp = _
  private var maxTime: Timestamp = _

  override def getMinTime: Timestamp = {
    if (minTime == null) {
      minTime = childBoundables.map(_.asInstanceOf[STBoundable].getMinTime).min(TimeUtils.timeOrdering)
    }
    minTime
  }

  override def getMaxTime: Timestamp = {
    if (maxTime == null) {
      maxTime = childBoundables.map(_.asInstanceOf[STBoundable].getMaxTime).max(TimeUtils.timeOrdering)
    }
    maxTime
  }
}

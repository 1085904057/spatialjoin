package org.apache.spark.spatialjoin.index.local.rtree3d

import java.sql.Timestamp

import org.apache.spark.spatialjoin.index.local.rtree.Boundable

/**
  * @author wangrubin3
  **/
trait STBoundable extends Boundable {
  def getMinTime: Timestamp

  def getMaxTime: Timestamp

  def centreT: Timestamp = new Timestamp((getMinTime.getTime + getMaxTime.getTime) / 2)
}

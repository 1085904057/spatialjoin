package org.apache.spark.spatialjoin.index.local.rtree3d

import java.sql.Timestamp

import org.apache.spark.spatialjoin.extractor.STExtractor
import org.apache.spark.spatialjoin.index.local.rtree.ItemBoundable

/**
  * @author wangrubin3
  **/
class STItemBoundable[T](item: T, extractor: STExtractor[T])
  extends ItemBoundable[T](item, extractor) with STBoundable {

  override def getMinTime: Timestamp = extractor.startTime(item)

  override def getMaxTime: Timestamp = extractor.endTime(item)
}

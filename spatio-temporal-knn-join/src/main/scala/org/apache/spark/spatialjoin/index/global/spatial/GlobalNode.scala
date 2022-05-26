package org.apache.spark.spatialjoin.index.global.spatial

/**
  * @author wangrubin3
  **/
trait GlobalNode extends Serializable {
  protected var partitionId: Int = -1

  def setPartitionId(id: Int): Unit = this.partitionId = id

  def getPartitionId: Int = this.partitionId
}

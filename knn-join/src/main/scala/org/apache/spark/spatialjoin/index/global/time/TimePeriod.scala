package org.apache.spark.spatialjoin.index.global.time

import java.sql.Timestamp

import org.apache.spark.spatialjoin.index.global.spatial.{GlobalIndex, GlobalQuad, GlobalRTree}
import org.locationtech.jts.geom.Envelope

/**
  * @author wangrubin3
  **/
case class TimePeriod(periodStart: Timestamp,
                      periodEnd: Timestamp,
                      density: Double) extends Serializable {

  private var lowerPartitionId: Int = _
  private var upperPartitionId: Int = _
  private var spatialIndex: GlobalIndex = _

  def buildSpatialIndex(samples: Array[Envelope],
                        globalBound: Envelope,
                        sampleRate: Double,
                        beta: Int, k: Int,
                        isQuadIndex: Boolean): Unit = {

    spatialIndex = if (isQuadIndex) {
      val index = new GlobalQuad(globalBound)
      index.build(samples, sampleRate, beta, k)
      index
    } else {
      val maxNumPerPartition = Math.max(10, samples.length / beta)
      val index = new GlobalRTree(maxNumPerPartition)
      index.build(samples)
      index
    }
  }

  def assignPartitionId(lowerId: Int): Int = {
    this.lowerPartitionId = lowerId
    this.upperPartitionId = spatialIndex.assignPartitionId(lowerId)
    this.upperPartitionId
  }

  def containsPartition(id: Int): Boolean = {
    id >= lowerPartitionId && id < upperPartitionId
  }

  def getPartitionEnv(partitionId: Int): Envelope = {
    val index = partitionId - lowerPartitionId
    spatialIndex.getLeafEnv(index)
  }

  def getSpatialIndex: GlobalIndex = spatialIndex
}

package org.apache.spark.spatialjoin

import org.apache.spark.Partitioner

class SpatialPartitioner(partitionNums: Int) extends Partitioner {
  override def numPartitions: Int = partitionNums

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
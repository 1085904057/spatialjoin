package org.apache.spark.spatialjoin

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.SamplingUtils
import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object SpatialDistanceJoin {

  def join[L: ClassTag, R: ClassTag](leftRdd: RDD[L], rightRdd: RDD[R],
                                     leftExtractor: L => Geometry,
                                     rightExtractor: R => Geometry,
                                     predicate: (Geometry, Geometry) => Boolean,
                                     distance: Double = 0.0): RDD[(L, R)] = {

    //get global info
    leftRdd.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val globalInfo = getGlobalInfo[L](leftRdd, leftExtractor)
      val numPartitions = getPartitionNum(leftRdd.getNumPartitions, rightRdd.getNumPartitions, globalInfo.getCount)
      val samples = getSamples[L](leftRdd, leftExtractor, numPartitions, globalInfo.getCount)
      val maxNumPerPartition = samples.length / numPartitions

      //construct spatial quadtree
      val partitionCollector = new ArrayBuffer[Envelope]()
      val globalEnv = new Envelope(globalInfo.getEnv)
      if (distance > 0.0) globalEnv.expandBy(distance)
      val root = new QuadNode(globalEnv)
      root.split(maxNumPerPartition, samples, partitionCollector)

      //spatial partition
      val broadcastQuadNode = leftRdd.sparkContext.broadcast(root)
      val partitioner = new SpatialPartitioner(partitionCollector.length)
      val leftSpatialPartitionedRdd =
        spatialPartition[L](leftRdd, leftExtractor, broadcastQuadNode, partitioner, distance)
      val rightSpatialPartitionedRdd =
        spatialPartition[R](rightRdd, rightExtractor, broadcastQuadNode, partitioner)

      //parallel spatial join
      val broadcastPartitionEnvs = leftRdd.sparkContext.broadcast(partitionCollector.toArray)
      leftSpatialPartitionedRdd.zipPartitions[R, (L, R)](rightSpatialPartitionedRdd) {
        (leftItems: Iterator[L], rightItems: Iterator[R]) => {
          val partitionId = TaskContext.getPartitionId()
          val partitionEnv = broadcastPartitionEnvs.value(partitionId)
          LocalJoin.join[L, R](
            leftItems, rightItems, leftExtractor, rightExtractor,
            partitionEnv, predicate, distance)
        }
      }
    } finally leftRdd.unpersist()

  }

  def selfJoin[I: ClassTag](rdd: RDD[I], extractor: I => Geometry,
                            predicate: (Geometry, Geometry) => Boolean,
                            distance: Double = 0.0): RDD[(I, I)] = {

    //get global info
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val globalInfo = getGlobalInfo[I](rdd, extractor)
      val numPartitions = getPartitionNum(rdd.getNumPartitions, rdd.getNumPartitions, globalInfo.getCount)
      val samples = getSamples[I](rdd, extractor, numPartitions, globalInfo.getCount)
      val maxNumPerPartition = samples.length / numPartitions

      //construct spatial quadtree
      val partitionCollector = new ArrayBuffer[Envelope]()
      val globalEnv = new Envelope(globalInfo.getEnv)
      if (distance > 0.0) globalEnv.expandBy(distance)
      val root = new QuadNode(globalEnv)
      root.split(maxNumPerPartition, samples, partitionCollector)

      //spatial partition
      val broadcastQuadNode = rdd.sparkContext.broadcast(root)
      val partitioner = new SpatialPartitioner(partitionCollector.length)
      val spatialPartitionedRdd = spatialPartition[I](rdd, extractor, broadcastQuadNode, partitioner, distance)

      //parallel spatial join
      val broadcastPartitionEnvs = rdd.sparkContext.broadcast(partitionCollector.toArray)
      spatialPartitionedRdd.mapPartitions(items => {
        val partitionId = TaskContext.getPartitionId()
        val partitionEnv = broadcastPartitionEnvs.value(partitionId)
        LocalJoin.selfJoin[I](items, extractor, partitionEnv, predicate, distance)
      })
    } finally rdd.unpersist()

  }

  private def getGlobalInfo[I](rdd: RDD[I], extractor: I => Geometry): GlobalInfo = {
    val add = (globalInfo: GlobalInfo, item: I) => globalInfo.add(extractor(item))
    val combine = (globalInfo: GlobalInfo, other: GlobalInfo) => globalInfo.combine(other)
    rdd.aggregate(new GlobalInfo(new Envelope(), 0L))(add, combine)
  }

  private def getPartitionNum(leftPartitionNum: Int, rightPartitionNum: Int, leftCount: Long): Int = {
    if (leftCount > leftPartitionNum * 2) {
      leftPartitionNum
    } else {
      if (leftCount > rightPartitionNum * 2) {
        rightPartitionNum
      } else {
        Math.max(1, (leftCount / 2).intValue())
      }
    }
  }

  private def getSamples[I](leftRdd: RDD[I], extractor: I => Geometry,
                            numPartitions: Int, totalNum: Long): Array[Envelope] = {

    val sampleSize = if (totalNum < 1000) totalNum.toInt
    else Math.max(numPartitions * 2, Math.min(totalNum / 100, Integer.MAX_VALUE)).toInt
    val fraction = SamplingUtils.computeFractionForSampleSize(sampleSize, totalNum, withReplacement = false)
    leftRdd.sample(withReplacement = false, fraction).map(extractor(_).getEnvelopeInternal).collect()
  }

  private def spatialPartition[T: ClassTag](rdd: RDD[T], extractor: T => Geometry,
                                            broadcastQuadNode: Broadcast[QuadNode],
                                            spatialPartitioner: SpatialPartitioner,
                                            distance: Double = 0.0): RDD[T] = {

    rdd.flatMap[(Int, T)](item =>
      broadcastQuadNode.value.getIds(extractor(item), distance).map((_, item))
    ).partitionBy(spatialPartitioner).map(_._2)
  }
}

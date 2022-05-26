package org.apache.spark.spatialjoin.join

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.spatialjoin.index.global.IndexNode
import org.apache.spark.spatialjoin.index.local.STRTree
import org.apache.spark.spatialjoin.{GlobalInfo, SpatialPartitioner}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.SamplingUtils
import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
 * @author wangrubin
 */
object SpatialKnnJoin {
  def join(leftRdd: RDD[Geometry], rightRdd: RDD[Geometry], k: Int): RDD[(Geometry, Array[Geometry])] = {

    //get global info
    rightRdd.persist(StorageLevel.MEMORY_AND_DISK)
    val rightGlobalInfo = getGlobalInfo(rightRdd)
    val numPartitions = getPartitionNum(leftRdd.getNumPartitions, rightRdd.getNumPartitions, rightGlobalInfo.getCount)
    val samples = getSamples(rightRdd, numPartitions, rightGlobalInfo.getCount)
    val maxNumPerPartition = samples.length / numPartitions

    //build global spatial index and collect partition envelopes
    val globalEnv = new Envelope(rightGlobalInfo.getEnv)
    val globalIndex = IndexNode(globalEnv)
    val leafNodes = globalIndex.split(maxNumPerPartition, samples, k)
    val partitioner = new SpatialPartitioner(leafNodes.length)

    //build index for right RDD and update global index by statistics
    val (rightIndexedRdd, numberOfPartition) = rightPartitionAndIndex(rightRdd, globalIndex, partitioner)
    for (index <- leafNodes.indices) {
      leafNodes(index).setNum(numberOfPartition.getOrElse(index, 0))
    }

    //broadcast
    val partitionEnvs = leafNodes.map(_.env)
    val broadcastPartitionEnvs = leftRdd.sparkContext.broadcast(partitionEnvs)
    val broadCastGlobalIndex = leftRdd.sparkContext.broadcast(globalIndex)

    //first round join
    val leftPartitionedRdd = leftFirstRoundPartition(leftRdd, broadCastGlobalIndex, partitioner, k)
    val joinRdd = leftPartitionedRdd.zipPartitions(rightIndexedRdd) {
      (leftGeoms: Iterator[Geometry], localIndex: Iterator[STRTree]) => {
        val partitionId = TaskContext.getPartitionId()
        val partitionEnv = broadcastPartitionEnvs.value(partitionId)
        LocalKnnJoin.firstRoundJoin(leftGeoms, localIndex.next(), (partitionEnv, partitionId), k)
      }
    }
    val firstRoundRdd = joinRdd.filter(_.isInstanceOf[FirstContainer]).map {
      case container: FirstContainer => (container.getGeom, container.getCandidates)
    }

    //second round join
    val deltaRdd = joinRdd.filter(_.isInstanceOf[SecondContainer]).zipWithUniqueId()
      .map {
        case (container: SecondContainer, id: Long) =>
          container.getGeomKey.bindId(id)
          container
      }
    val deltaDistanceRdd = deltaRdd.map {
      container: SecondContainer =>
        container.getGeomKey.asInstanceOf[GeomKeyWithParam]
    }

    val deltaPartitionRdd = leftSecondRoundPartition(deltaDistanceRdd, broadCastGlobalIndex, partitioner)
    val deltaJoinRdd = deltaPartitionRdd.zipPartitions(rightIndexedRdd) {
      (leftGeoms: Iterator[GeomKeyWithParam], localIndex: Iterator[STRTree]) => {
        val partitionEnvs = broadcastPartitionEnvs.value
        LocalKnnJoin.secondRoundJoin(leftGeoms, localIndex.next(), partitionEnvs)
      }
    }

    val secondRoundRdd = deltaRdd.union(deltaJoinRdd).map {
      container => (container.getGeomKey, container.getCandidatesWithDist)
    }.groupByKey().map {
      case (geometryKey, candidateIterator) =>
        val leftGeom = geometryKey.getGeom
        val rightGeoms = candidateIterator.toArray
          .foldLeft(Array.empty[(Geometry, Double)])((a, b) => a ++ b)
          .sortBy(_._2).map(_._1)
        (leftGeom, rightGeoms.slice(0, Math.min(k, rightGeoms.length)))
    }

    //union two round result
    firstRoundRdd.union(secondRoundRdd)
  }

  private def getGlobalInfo(rdd: RDD[Geometry]): GlobalInfo = {
    val add = (globalInfo: GlobalInfo, geom: Geometry) => globalInfo.add(geom)
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

  private def getSamples(rdd: RDD[Geometry], numPartitions: Int, totalNum: Long): Array[Envelope] = {
    val sampleSize = if (totalNum < 1000) totalNum.toInt
    else Math.max(numPartitions * 2, Math.min(totalNum / 100, Integer.MAX_VALUE)).toInt
    val fraction = SamplingUtils.computeFractionForSampleSize(sampleSize, totalNum, withReplacement = false)
    rdd.sample(withReplacement = false, fraction).map(_.getEnvelopeInternal).collect()
  }

  private def rightPartitionAndIndex(rdd: RDD[Geometry],
                                     globalIndex: IndexNode,
                                     spatialPartitioner: SpatialPartitioner): (RDD[STRTree], Map[Int, Int]) = {
    //broadcast and accumulator
    val broadcastQuadNode: Broadcast[IndexNode] = rdd.sparkContext.broadcast(globalIndex)
    val statistic = rdd.sparkContext.collectionAccumulator[(Int, Int)]("")

    //spatial partition
    val partitionedRdd = rdd.flatMap(geom => broadcastQuadNode.value.bindIds(geom).map((_, geom)))
      .partitionBy(spatialPartitioner).map(_._2)

    //build local index and collect statistics
    val indexedRdd = partitionedRdd.mapPartitions(geomIter => {
      val geoms = geomIter.toArray
      statistic.add((TaskContext.getPartitionId(), geoms.length))
      val spatialIndex = new STRTree()
      for (rightGeom <- geoms) {
        spatialIndex.insert(rightGeom.getEnvelopeInternal, rightGeom)
      }
      Iterator(spatialIndex)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    //trigger calculation
    indexedRdd.count()
    rdd.unpersist(false)

    //return
    (indexedRdd, statistic.value.asScala.toMap)
  }

  private def leftFirstRoundPartition(rdd: RDD[Geometry], broadcastQuadNode: Broadcast[IndexNode],
                                      spatialPartitioner: SpatialPartitioner, k: Int): RDD[Geometry] = {

    rdd.map(geom => {
      val id = broadcastQuadNode.value.bindId(geom, k)
      (id, geom)
    }).partitionBy(spatialPartitioner).map(_._2)
  }

  private def leftSecondRoundPartition(rdd: RDD[GeomKeyWithParam], broadcastQuadNode: Broadcast[IndexNode],
                                       spatialPartitioner: SpatialPartitioner): RDD[GeomKeyWithParam] = {

    rdd.mapPartitions(leftGeomIter => {
      val buffer = new ArrayBuffer[(Int, GeomKeyWithParam)]()
      for (geomKeyWithParam <- leftGeomIter) {
        broadcastQuadNode.value.bindIds(geomKeyWithParam.getGeom, geomKeyWithParam.getExpandDist)
          .filter(_ != geomKeyWithParam.getPartitionId)
          .foreach(rePartitionId => {
            buffer += ((rePartitionId, geomKeyWithParam))
          })
      }
      buffer.toIterator
    }).partitionBy(spatialPartitioner).map(_._2)
  }
}

package org.apache.spark.spatialjoin.join

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.spatialjoin.extractor.STExtractor
import org.apache.spark.spatialjoin.index.global.time.STIndex
import org.apache.spark.spatialjoin.index.local.time.{STRtree, TRCBasedBins}
import org.apache.spark.spatialjoin.partition.GlobalSTInfo
import org.apache.spark.spatialjoin.utils.TimeUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.CollectionAccumulator
import org.locationtech.jts.geom.Envelope

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * @author wangrubin3
  * @desc if use rtree as spatial partition index, set isQuadIndex as false
  **/
class SpatioTemporalKnnJoin(deltaMilli: Long, k: Int, alpha: Int, beta: Int, binNum: Int, isQuadIndex: Boolean = true) extends Serializable {

  def join[R: ClassTag, S: ClassTag](leftRdd: RDD[R],
                                     rightRdd: RDD[S],
                                     leftExtractor: STExtractor[R],
                                     rightExtractor: STExtractor[S]): String = {

    val originalTime = System.currentTimeMillis()
    val spark = leftRdd.sparkContext

    //left global info
    leftRdd.persist(StorageLevel.MEMORY_AND_DISK)
    val leftGlobalInfo = GlobalSTInfo.doStatistic(leftRdd, leftExtractor)
    val leftTimeRange = TimeUtils.expandTimeRange(leftGlobalInfo.getTimeRange, deltaMilli)

    //right global info
    val validateRightRdd = rightRdd.filter(row => {
      val timeRange = (rightExtractor.startTime(row), rightExtractor.endTime(row))
      TimeUtils.isIntersects(leftTimeRange, timeRange)
    }).persist(StorageLevel.MEMORY_AND_DISK)
    val rightGlobalInfo = GlobalSTInfo.doStatistic(validateRightRdd, rightExtractor)

    //sample and build global index and partitioner
    val globalBound = new Envelope(rightGlobalInfo.getEnv)
    val globalRange = rightGlobalInfo.getTimeRange
    val globalIndex = new STIndex(globalBound, globalRange, alpha, beta, deltaMilli, k, isQuadIndex)
    val (samples, sampleRate) = JoinUtils.sample(validateRightRdd, rightExtractor, rightGlobalInfo.getCount, alpha, beta)
    val partitionNum = globalIndex.build(samples, sampleRate)
    val partitioner = JoinUtils.getPartitioner(partitionNum)

    //repartition right rdd
    var bcGlobalIndex = spark.broadcast(globalIndex)
    val partitionedRightRdd = validateRightRdd.flatMap(rightRow => {
      val geom = rightExtractor.geom(rightRow)
      val startTime = rightExtractor.startTime(rightRow)
      val endTime = rightExtractor.endTime(rightRow)
      bcGlobalIndex.value.getPartitionIds(geom, startTime, endTime).map((_, rightRow))
    }).partitionBy(partitioner).map(_._2).mapPartitions(rowIter => {
      Iterator((TaskContext.getPartitionId(), rowIter.toArray))
    }).filter(_._2.nonEmpty)

    //collect time bin of each right partition
    val rightCopyAccum = spark.longAccumulator
    val partitionBoundAccum: CollectionAccumulator[(Int, Envelope)] =
    if (!isQuadIndex) spark.collectionAccumulator[(Int, Envelope)]("") else null
    val timeBinMap = partitionedRightRdd.map {
      case (partitionId, rightRows) =>
        rightCopyAccum.add(rightRows.length)
        val bound = new Envelope()
        val timeBin = new TRCBasedBins(binNum, k)
        val timeRanges = rightRows.map(row => {
          if (!isQuadIndex) bound.expandToInclude(rightExtractor.geom(row).getEnvelopeInternal)
          (rightExtractor.startTime(row), rightExtractor.endTime(row))
        })
        timeBin.build(timeRanges)
        if (!isQuadIndex) partitionBoundAccum.add((partitionId, bound))
        (partitionId, timeBin)
    }.collect().toMap
    val rightCopyNum = rightCopyAccum.value
    val bcTimeBinMap = spark.broadcast(timeBinMap)
    if (!isQuadIndex) {
      globalIndex.updateBound(partitionBoundAccum.value.asScala.toMap)
      bcGlobalIndex = spark.broadcast(globalIndex)
    }

    //build index for right rdd
    val indexedRightRdd = partitionedRightRdd.map {
      case (partitionId, rightRows) =>
        val localIndex = new STRtree[S](rightExtractor, k, binNum)
        localIndex.build(rightRows)
        (partitionId, localIndex)
    }.persist(StorageLevel.MEMORY_AND_DISK)

    val localJoin = new LocalJoin[R, S](leftExtractor, rightExtractor, deltaMilli, k)

    //first round spatial partition for left rdd
    val leftPartitionedRdd = leftRdd.zipWithUniqueId().flatMap {
      case (leftRow, id) =>
        val geom = leftExtractor.geom(leftRow)
        val startTime = leftExtractor.startTime(leftRow)
        val endTime = leftExtractor.endTime(leftRow)
        val index = bcGlobalIndex.value
        if (!isQuadIndex) assert(index.isUpdate)
        val partitionId = index.getPartitionId(geom, (startTime, endTime), bcTimeBinMap.value)
        if (partitionId == -1) {
          val rowWithId = RowWithId(-id, leftRow)
          val tempPartitionId = index.getPartitionIds(geom, startTime, endTime, Double.MaxValue).head
          Iterator((tempPartitionId, rowWithId))
        } else Iterator((partitionId, RowWithId(id, leftRow)))
    }.partitionBy(partitioner).map(_._2)

    val hitAccum = spark.longAccumulator
    val leftCopyAccum = spark.longAccumulator

    //first round spatio-temporal knn join computation
    val leftRepartitionedRdd = leftPartitionedRdd.zipPartitions(indexedRightRdd) {
      case (leftRows: Iterator[RowWithId[R]], rightIndex: Iterator[(Int, STRtree[S])]) =>
        if (rightIndex.nonEmpty) {
          val (partitionId, localIndex) = rightIndex.next()
          val index = bcGlobalIndex.value
          if (!isQuadIndex) assert(index.isUpdate)
          localJoin.firstRoundJoin(leftRows, localIndex, index.getPartition(partitionId), partitionId)
        } else Iterator.empty
    }.flatMap {
      case (rowWithId@RowWithId(id, leftRow), container: ApproximateContainer[S]) =>
        val leftGeom = leftExtractor.geom(leftRow)
        val startTime = leftExtractor.startTime(leftRow)
        val endTime = leftExtractor.endTime(leftRow)
        val index = bcGlobalIndex.value
        if (!isQuadIndex) assert(index.isUpdate)
        val partitionIds = index.getPartitionIds(leftGeom, startTime, endTime, container.expandDist)
        leftCopyAccum.add(partitionIds.length)
        if (id > 0 && partitionIds.length == 1) hitAccum.add(1)
        partitionIds.map(partitionId => {
          if (id > 0 && partitionId == container.partitionId) (partitionId, (rowWithId, container)) //reserve first round knn query result
          else (partitionId, (rowWithId, ApproximateContainer[S](Array.empty, -1, container.expandDist)))
        })
    }.partitionBy(partitioner).map(_._2)

    //second round spatio-temporal knn join computation and result merge
    leftRepartitionedRdd.zipPartitions(indexedRightRdd) {
      (leftRows: Iterator[(RowWithId[R], ApproximateContainer[S])], rightIndex: Iterator[(Int, STRtree[S])]) => {
        if (rightIndex.nonEmpty) {
          val (partitionId, localIndex) = rightIndex.next()
          localJoin.secondRoundJoin(leftRows, localIndex, bcGlobalIndex.value.getPartition(partitionId))
        } else Iterator.empty
      }
    }.groupByKey().map {
      case (RowWithId(_, leftRow), candidateIter) =>
        val candidates = candidateIter.foldLeft(Array.empty[(S, Double)])((a, b) => a ++ b)
          .sortBy(_._2).map(_._1)
        val kNN = candidates.slice(0, Math.min(k, candidates.length))
        (leftRow, kNN)
    }.count()

    val totalTime = (System.currentTimeMillis() - originalTime) / 1E3
    //s"total_time:${totalTime.toString}seconds"

    val leftNum = leftGlobalInfo.getCount
    val rightNum = rightGlobalInfo.getCount
    val leftCopyNum = leftCopyAccum.value
    val hitNum = hitAccum.value
    val leftCopyRate = leftCopyNum.toDouble / leftNum
    val rightCopyRate = rightCopyNum.toDouble / rightNum
    val hitRate = hitNum.toDouble / leftNum
    val statistic = s"leftNum:$leftNum" +
      s"rightNum:$rightNum" +
      s"leftCopyNum:$leftCopyNum" +
      s"rightCopyNum:$rightCopyNum" +
      s"leftCopyRate:$leftCopyRate" +
      s"rightCopyRate:$rightCopyRate" +
      s"hitNum:$hitNum" +
      s"hitRate:$hitRate"

    Seq(s"totalTime:$totalTime", statistic).mkString("\n")
  }
}

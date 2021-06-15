package org.apache.spark.spatialjoin.index.global.time

import java.sql.Timestamp

import org.apache.spark.spatialjoin.index.local.time.TRCBasedBins
import org.apache.spark.spatialjoin.utils.TimeUtils
import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class STIndex(globalEnv: Envelope,
              globalRange: (Timestamp, Timestamp),
              alpha: Int, beta: Int,
              deltaMilli: Long, k: Int,
              isQuadIndex: Boolean) extends Serializable {

  private val timePeriods = new ArrayBuffer[TimePeriod]()
  private var isUpdated: Boolean = false

  def isUpdate: Boolean = this.isUpdated

  def updateBound(leafNodeMap: Map[Int, Envelope]): Unit = this.synchronized {
    if (!isUpdated) {
      this.timePeriods.foreach(period => period.getSpatialIndex.updateBound(leafNodeMap))
      this.isUpdated = true
    }
  }

  def build(samples: Array[STBound], sampleRate: Double): Int = {
    val (minTime, maxTime) = globalRange
    val sortedSamples = samples.sortBy(_.startTime.getTime)
    val avg = sortedSamples.map(sample => sample.endTime.getTime - sample.startTime.getTime).sum.toDouble / samples.length
    val minSpanMilli = Math.max(Math.max(2 * deltaMilli, avg), (maxTime.getTime - minTime.getTime) / alpha)
    val minSampleNum = Math.max(samples.length / alpha, sampleRate * beta * k)

    var timeSpan = 0L
    var sweepLine, periodStart = minTime
    var sampleHolder = new ArrayBuffer[STBound]()
    for (sample <- sortedSamples) {
      timeSpan += sample.startTime.getTime - sweepLine.getTime
      sweepLine = sample.startTime
      sampleHolder += sample
      if (timeSpan >= minSpanMilli && sampleHolder.length >= minSampleNum) {
        val density = sampleHolder.length.toDouble / timeSpan
        val period = TimePeriod(periodStart, sweepLine, density)
        period.buildSpatialIndex(sampleHolder.map(_.env).toArray, globalEnv, sampleRate, beta, k, isQuadIndex)
        timePeriods += period
        periodStart = sweepLine
        sampleHolder = sampleHolder.filter(_.endTime.after(sweepLine))
        timeSpan = 0
      }
    }

    timeSpan += maxTime.getTime - sweepLine.getTime
    val density = sampleHolder.length.toDouble / timeSpan
    val period = TimePeriod(periodStart, maxTime, density)
    period.buildSpatialIndex(sampleHolder.map(_.env).toArray, globalEnv, sampleRate, beta, k, isQuadIndex)
    timePeriods += period

    val partitionIdNum = timePeriods.foldLeft(0)(
      (baseId: Int, period: TimePeriod) => period.assignPartitionId(baseId)
    )
    partitionIdNum
  }

  def getPartitionId(queryGeom: Geometry,
                     queryRange: (Timestamp, Timestamp),
                     timeBinMap: Map[Int, TRCBasedBins]): Int = {

    val expandQueryRange = TimeUtils.expandTimeRange(queryRange, deltaMilli)
    var partitionId = -1
    for (period <- getTimePeriods(expandQueryRange) if partitionId == -1) {
      partitionId = period.getSpatialIndex.getPartitionId(queryGeom, expandQueryRange, timeBinMap)
    }
    partitionId
  }

  //for dataset S
  def getPartitionIds(geom: Geometry, start: Timestamp, end: Timestamp): Array[Int] = {
    val queryRange = (start, end)
    if (isQuadIndex) {
      (for (period <- getTimePeriods(queryRange)) yield {
        period.getSpatialIndex.getPartitionIds(geom)
      }).foldLeft(Array.empty[Int])((a, b) => a ++ b)
    } else {
      for (period <- getTimePeriods(queryRange)) yield {
        period.getSpatialIndex.getPartitionId(geom)
      }
    }
  }

  //for dataset R in second round
  def getPartitionIds(geom: Geometry, start: Timestamp, end: Timestamp, distance: Double): Array[Int] = {
    val expandQueryRange = TimeUtils.expandTimeRange((start, end), deltaMilli)

    (for (period <- getTimePeriods(expandQueryRange)) yield {
      period.getSpatialIndex.getPartitionIds(geom, distance)
    }).foldLeft(Array.empty[Int])((a, b) => a ++ b)
  }

  def getPartition(partitionId: Int): STBound = {
    val period = timePeriods.find(_.containsPartition(partitionId)).get
    val env = period.getPartitionEnv(partitionId)
    STBound(env, period.periodStart, period.periodEnd)
  }

  private def getTimePeriods(queryRange: (Timestamp, Timestamp)): Array[TimePeriod] = {
    val startIndex = timePeriods.indexWhere(_.periodEnd.after(queryRange._1))
    var endIndex = timePeriods.indexWhere(_.periodEnd.after(queryRange._2), startIndex)
    if (endIndex == -1) endIndex = timePeriods.length else endIndex += 1
    timePeriods.slice(startIndex, endIndex).toArray
  }
}

package org.apache.spark.spatialjoin.index.local.time

import java.sql.Timestamp

import org.apache.spark.spatialjoin.utils.TimeUtils

import scala.collection.mutable.ArrayBuffer

/**
  * @author wangrubin3
  **/
class TRCBasedBins(binNum: Int, k: Int) {

  private var total: Int = _
  private var startTime: Timestamp = _
  private var endTime: Timestamp = _
  private var spanMilli: Long = _
  private val leftMaxSums = new ArrayBuffer[Int](binNum)
  private val rightMinSums = new ArrayBuffer[Int](binNum)

  def build(timeRanges: Array[(Timestamp, Timestamp)]): Unit = {
    total = timeRanges.length
    startTime = timeRanges.map(_._1).min(TimeUtils.timeOrdering)
    endTime = timeRanges.map(_._2).max(TimeUtils.timeOrdering)
    spanMilli = Math.ceil((endTime.getTime - startTime.getTime + 1).toDouble / binNum).toLong
    val startMilli = startTime.getTime

    //calculate count
    val minNums = Array.ofDim[Int](binNum)
    timeRanges.map(_._1).foreach(time => {
      val index = ((time.getTime - startMilli) / spanMilli).toInt
      minNums(index) = minNums(index) + 1
    })
    val maxNums = Array.ofDim[Int](binNum)
    timeRanges.map(_._2).foreach(time => {
      val index = ((time.getTime - startMilli) / spanMilli).toInt
      maxNums(index) = maxNums(index) + 1
    })

    //aggregate sum
    minNums.foldRight(rightMinSums)((minNum, minSums) => {
      minSums += minSums.lastOption.getOrElse(0) + minNum
      minSums
    })
    maxNums.foldLeft(leftMaxSums)((maxSums, maxNum) => {
      maxSums += maxSums.lastOption.getOrElse(0) + maxNum
      maxSums
    })
  }

  def hasKnn(queryRange: (Timestamp, Timestamp)): Boolean = {
    if (queryRange._1.after(endTime) || queryRange._2.before(startTime)) return false

    val minThanStart = if (queryRange._1.after(startTime)) {
      val startIndex = (queryRange._1.getTime - startTime.getTime) / spanMilli
      leftMaxSums(startIndex.toInt)
    } else 0

    val maxThanEnd = if (queryRange._2.before(endTime)) {
      val endIndex = (queryRange._2.getTime - startTime.getTime) / spanMilli
      rightMinSums(binNum - endIndex.toInt - 1)
    } else 0

    (total - minThanStart - maxThanEnd) >= k
  }
}

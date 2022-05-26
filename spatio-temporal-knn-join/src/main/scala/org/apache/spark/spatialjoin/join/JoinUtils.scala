package org.apache.spark.spatialjoin.join

import org.apache.spark.Partitioner
import org.apache.spark.spatialjoin.extractor.STExtractor
import org.apache.spark.spatialjoin.index.global.time.STBound
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.SamplingUtils

/**
  * @author wangrubin3
  **/
object JoinUtils {
  def getPartitioner(partitionNum: Int): Partitioner = new Partitioner {
    override def numPartitions: Int = partitionNum

    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }

  def sample[T](rdd: RDD[T], extractor: STExtractor[T],
                totalNum: Long, alpha: Int, beta: Int): (Array[STBound], Double) = {

    val transfer = (row: T) => {
      val env = extractor.geom(row).getEnvelopeInternal
      STBound(env, extractor.startTime(row), extractor.endTime(row))
    }

    val numPartitions = alpha * beta
    val sampleSize = if (totalNum < 1000) totalNum.toInt
    else Math.max(numPartitions * 2, Math.min(totalNum / 100, Integer.MAX_VALUE)).toInt
    val fraction = SamplingUtils.computeFractionForSampleSize(sampleSize, totalNum, withReplacement = false)
    val samples = rdd.sample(withReplacement = false, fraction).map(transfer).collect()
    (samples, fraction)
  }
}

package org.apache.spark.spatialjoin.app

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.spatialjoin.join.SpatialKnnJoin
import org.apache.spark.spatialjoin.serialize.GeometryKryoRegistrator
import org.apache.spark.spatialjoin.utils.WKTUtils

/**
 * @author wangrubin
 */
object SpatialJoinApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.driver.maxResultSize", "5g")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[GeometryKryoRegistrator].getName)
      .setAppName("SpatialJoinApp")
      //.setMaster("local[*]")
    val spark = new SparkContext(conf)

    val time = System.currentTimeMillis()

    val storePath = args(0)
    val leftFileName = args(1)
    val rightFileName = args(2)
    val k = args(3).toInt
    val numPartitions = 1024

    val readRdd = (rightFileName: String) => spark.textFile(rightFileName, numPartitions)
      .map(line => WKTUtils.read(line.split(",")(4)))
    val pairNum = SpatialKnnJoin.join(readRdd(leftFileName), readRdd(rightFileName), k).count()

    val totalTime = (System.currentTimeMillis() - time) / 1E3
    val statistic = s"total_time:$totalTime;pair_num:$pairNum"
    println(statistic + "**************************************")
    spark.parallelize(Seq(statistic))
      .repartition(1)
      .saveAsTextFile(storePath)

    spark.stop()
  }
}

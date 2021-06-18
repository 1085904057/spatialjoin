package org.apache.spark.sql.simba.examples

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Point

object KnnJoin {

  case class PointData(x: Double, y: Double, tr: TimeRange = null, globalTimeRange: Long = 0, k: Int = 5, threshold: Long = 0) {
    def isIntersects(outer: TimeRange): Boolean = {
      val extendStartTime = tr.startTime.getTime - threshold
      val extendEndTime = tr.endTime.getTime + threshold
      !(outer.startTime.getTime > extendEndTime || outer.endTime.getTime < extendStartTime)
    }
  }

  case class TimeRange(startTime: Timestamp, endTime: Timestamp) {
    def getDeltaTime(): Long = endTime.getTime - startTime.getTime
    def getDeltaTime(threshold: Long): Long = 2 * threshold

    def isIntersects(outer: TimeRange, threshold: Long): Boolean = {
      val extendStartTime = this.startTime.getTime - threshold
      val extendEndTime = this.endTime.getTime + threshold
      !(outer.startTime.getTime > extendEndTime || outer.endTime.getTime < extendStartTime)
    }
  }

  def isIntersects(left: Timestamp, right: Timestamp, threshold: Long): Boolean = {
    left.getTime > right.getTime - threshold && left.getTime < right.getTime + threshold
  }

  def main(args: Array[String]): Unit = {

    val simbaSession =
      SimbaSession
        .builder()
//        .master("local[*]")
        .appName("Simba kNN Join")
        .config("simba.join.partitions", args(5))
        .config("simba.index.partitions", args(6))
        .getOrCreate()
    knnJoin(simbaSession, args)
    simbaSession.stop()
  }

  def knnJoin(simba: SimbaSession, args: Array[String]) {

    import simba.implicits._
    import simba.simbaImplicits._

    val leftFile = "hdfs://just" + args(0) //"D:\\spatialjoin_test\\left.csv"
    val rightFile = "hdfs://just" + args(1)  //"D:\\spatialjoin_test\\right.csv"
    val storeFile = "hdfs://just" + args(2)

//        val leftFile = "D:\\spatialjoin_test\\left.csv"
//        val rightFile = "D:\\spatialjoin_test\\right.csv"
    val k = args(3).toInt
    val threshold = args(4).toLong

    val startTime = System.currentTimeMillis()

    val DS1 = simba.read.textFile(leftFile).map { line =>
      val parts = line.split(",")
      val point = WKTUtils.read(parts.last).asInstanceOf[Point]
      PointData(point.getX, point.getY, TimeRange(Timestamp.valueOf(parts(3)), Timestamp.valueOf(parts(3))))
    }.repartition(200).toDF()

    val DS2 = simba.read.textFile(rightFile).map { line =>
      val parts = line.split(",")
      val point = WKTUtils.read(parts.last).asInstanceOf[Point]
      PointData(point.getX, point.getY, TimeRange(Timestamp.valueOf(parts(3)), Timestamp.valueOf(parts(3))))
    }.repartition(200).toDF()

    DS2.index(RTreeType, "RtreeForDS2", Array("x", "y"))
    val joinStartTime = System.currentTimeMillis()

    // Embed timers inside RKJSpark for partitioning time and indexing time
    val count = DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), k)
      .rdd.filter{ i =>
        val values = i.toSeq
        val left = values(2).asInstanceOf[GenericRowWithSchema].values.head.asInstanceOf[Timestamp]
        val right = values(8).asInstanceOf[GenericRowWithSchema].values.head.asInstanceOf[Timestamp]
        isIntersects(left, right, threshold)
      }.count()

    val totalTime = System.currentTimeMillis() - startTime
    val joinTime = System.currentTimeMillis() - joinStartTime

    val statistic = s"total_time:$totalTime;join_time:$joinTime;pair_num:$count"

    simba.sparkContext.parallelize(Seq(statistic))
      .repartition(1)
      .saveAsTextFile(storeFile)

    simba.stop()
  }
}
package cs.purdue.edu.just.examples

import java.sql.Timestamp

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialindex.rtree.Point
import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object STKnnJoinApp {

  def main(args: Array[String]): Unit = {
    val leftFile = "hdfs://just" + args(0)
    val rightFile = "hdfs://just" + args(1)
    val storeFile = "hdfs://just" + args(2)
    val k = args(3).toInt
    val globalStartTime = Timestamp.valueOf(args(4))
    val globalEndTime = Timestamp.valueOf(args(5))
    val threshold = args(6).toLong

    //        val leftFile = "D:\\spatialjoin_test\\left_small.csv"
    //        val rightFile = "D:\\spatialjoin_test\\right_small.csv"
    //        val storeFile = "hello world"
    //        val k = 5
    //        val globalStartTime = Timestamp.valueOf("2021-04-25 00:00:00.000")
    //        val globalEndTime = Timestamp.valueOf("2021-04-29 00:00:00.000")
    //        val threshold = 1 * 60 * 1000

    val startTime = System.currentTimeMillis()
    val globalDeltaTime: Long = globalEndTime.getTime - globalStartTime.getTime
    val conf = new SparkConf()
      .setAppName("App for Spatial Temporal Knn JOIN")
    //      .setMaster("local[*]")
    val spark = new SparkContext(conf)

    val basepoints: RDD[(Point, String)] = spark.textFile(rightFile, 1024).map(line => {
      val attrs = line.split(",")
      val geom = new WKTReader().read(attrs.last)
      val startTime = Timestamp.valueOf(attrs(3))
      val endTime = Timestamp.valueOf(attrs(3))
      (Point(geom.getCoordinates.head.x.toFloat, geom.getCoordinates.head.y.toFloat,
        TimeRange(startTime, endTime), globalDeltaTime, k, threshold), "1")
    })

    val baseLocationRDD: SpatialRDD[Point, String] = SpatialRDD(basepoints).cache()

    val querypoints: RDD[Point] = spark.textFile(leftFile, 1024).map(line => {
      val attrs = line.split(",")
      val geom = new WKTReader().read(attrs.last)
      val startTime = Timestamp.valueOf(attrs(3))
      val endTime = Timestamp.valueOf(attrs(3))
      Point(geom.getCoordinates.head.x.toFloat, geom.getCoordinates.head.y.toFloat,
        TimeRange(startTime, endTime), globalDeltaTime, k, threshold)
    })

    val joinStartTime = System.currentTimeMillis()

    val knnjoin = new JustKnnJoinRDD(baseLocationRDD, querypoints, k, _ => true, _ => true)

    val knnjoinresult: RDD[(Point, Iterator[(Point, String)])] = knnjoin.rangebasedKnnjoin()
    val resultNum = knnjoinresult.flatMap { item =>
      item._2.map { tuple =>
        (item._1, tuple._1)
      }.filter{ i=> i._1.isIntersects(i._2.tr) }
    }.count()

    val totalTime = System.currentTimeMillis() - startTime
    val joinTime = System.currentTimeMillis() - joinStartTime

    val statistic = s"total_time:$totalTime;join_time:$joinTime;pair_num:$resultNum"

    basepoints.sparkContext.parallelize(Seq(statistic))
      .repartition(1)
      .saveAsTextFile(storeFile)

    spark.stop()
  }

}

package cs.purdue.edu.examples

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialindex.rtree.Point
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.{Util, knnJoinRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by merlin on 6/8/16.
  */
object KnnJoinApp {
  def main(args: Array[String]) {

    val leftFile = "hdfs://just" + args(0) //"file:///E:/毕业设计/数据/hotel_restaurant_knn/restaurant.csv"
    val rightFile = "hdfs://just" + args(1) //"file:///E:/毕业设计/数据/hotel_restaurant_knn/hotel.csv"
    val storeFile = "hdfs://just" + args(2)
    //Util.localIndex
    val knn = args(3).toInt

    val conf = new SparkConf()
      .setAppName("App for Spatial Knn JOIN")
    //.setMaster("local[*]")
    val spark = new SparkContext(conf)

    val begin = System.currentTimeMillis()
    /** **********************************************************************************/
    val leftpoints: RDD[(Point, String)] = spark.textFile(leftFile, 1024).map(wkt => {
      Try(new WKTReader().read(wkt.split(",").last))
    }).filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }
    val leftLocationRDD = SpatialRDD(leftpoints).cache()

    /** **********************************************************************************/

    /** **********************************************************************************/
    val rightpoints: RDD[Point] = spark.textFile(rightFile, 1024).map(wkt => {
      Try(new WKTReader().read(wkt.split(",").last))
    }).filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat))
    }
    /** **********************************************************************************/

    var b1 = System.currentTimeMillis

    val knnjoin = new knnJoinRDD[Point, String](leftLocationRDD, rightpoints, knn, (id) => true, (id) => true)

    val knnjoinresult = knnjoin.rangebasedKnnjoin()

    val tuples = knnjoinresult.map { case (b, v) => (1, v.size) }.reduceByKey { case (a, b) => {
      a + b
    }
    }.map { case (a, b) => b }.collect()

    println("the outer table size: " + rightpoints.count())
    println("the inner table size: " + leftpoints.count())

    println("global index: " + Util.localIndex + " ; local index: " + Util.localIndex)
    println("the k value for kNN join: " + knn)
    println("knn join results size: " + tuples(0))
    println("spatial kNN join time: " + (System.currentTimeMillis - b1) + " (ms)")

    val totalTime = (System.currentTimeMillis() - begin) / 1E3
    val joinTime = (System.currentTimeMillis - b1) / 1E3
    val statistic = s"total_time:$totalTime;join_time:$joinTime;pair_num:${tuples(0)}"
    println(statistic + "**************************************")
    rightpoints.sparkContext.parallelize(Seq(statistic))
      .repartition(1)
      .saveAsTextFile(storeFile)

    spark.stop()

  }

}

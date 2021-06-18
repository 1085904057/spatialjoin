package cs.purdue.edu.examples

import com.vividsolutions.jts.io.WKTReader
//import cs.purdue.cs.purdue.edu.spatialindex.qtree.{Box, Point}
import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.Util
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object SpatialJoins {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("LocationSpark Spatial Joins")
    val sc = new SparkContext(conf)

    sc.setLogLevel("OFF")
    Util.localIndex = "QTREE"

    //左边rectangle，右边point
    val leftPath = args(0)
    val rightPath = args(1)
    val storePath = args(2)

    def aggfunction1[K, V](itr: Iterator[(K, V)]): Int = {
      itr.size
    }

    def aggfunction2(v1: Int, v2: Int): Int = {
      v1 + v2
    }

    val begin = System.currentTimeMillis()
    val leftpoints = sc.textFile(rightPath, 1024).map(x => (Try(new WKTReader().read(x)))).filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }
    val leftLocationRDD = SpatialRDD(leftpoints).cache()

    val rightData = sc.textFile(leftPath)
    val rightBoxes = rightData.map(x => (Try(new WKTReader().read(x)))).filter(_.isSuccess).map {
      case x =>
        val env = x.get.getEnvelopeInternal
        Box(env.getMinX.toFloat, env.getMinY.toFloat, env.getMaxX.toFloat, env.getMaxY.toFloat)
    }

    val t0 = System.currentTimeMillis()
    val joinresultRdd0 = leftLocationRDD.rjoin(rightBoxes)(aggfunction1, aggfunction2)
    val tuples0 = joinresultRdd0.map { case (b, v) => (1, v) }.reduceByKey { case (a, b) => {
      a + b
    }
    }.map { case (a, b) => b }.collect()
    val count0 = tuples0(0)

    val join_time = (System.currentTimeMillis() - t0) / 1E3
    val total_time = (System.currentTimeMillis() - begin) / 1E3

    val statistics = s"total_time:$total_time;join_time:$join_time;pair_num:$count0"
    sc.parallelize(Seq(statistics)).repartition(1)
      .saveAsTextFile(storePath)
  }
}
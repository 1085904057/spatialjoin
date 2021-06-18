package cs.purdue.edu.examples

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.Util
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by merlin on 6/8/16.
  */
object SpatialJoinApp {
  def main(args: Array[String]) {
    val leftFile = "file:///E:/毕业设计/数据/china/build_poi_distance/gis_osm_pois_free_1.csv"
    val rightFile = "file:///E:/毕业设计/数据/china/build_poi_distance/gis_osm_traffic_a_free_1.csv"
    //Util.localIndex

    val conf = new SparkConf()
      .setAppName("Test for Spatial JOIN SpatialRDD")
      .setMaster("local[*]")

    val spark = new SparkContext(conf)

    /** **********************************************************************************/
    //this is for WKT format for the left data points
    val leftpoints = spark.textFile(leftFile).map(x => {
      Try(new WKTReader().read(x.split("\t")(0)))
    })
      .filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }
    val leftLocationRDD = SpatialRDD(leftpoints).cache()

    /** **********************************************************************************/

    /** **********************************************************************************/
    val rightData = spark.textFile(rightFile)
    val rightBoxes = rightData.map(x => {
      val env = new WKTReader().read(x.split("\t")(0)).getEnvelopeInternal
      Box(env.getMinX.toFloat, env.getMinY.toFloat, env.getMaxX.toFloat, env.getMaxY.toFloat)
    })

    def aggfunction1[K, V](itr: Iterator[(K, V)]): Int = {
      itr.size
    }

    def aggfunction2(v1: Int, v2: Int): Int = {
      v1 + v2
    }

    /** **********************************************************************************/
    var b1 = System.currentTimeMillis

    val joinresultRdd = leftLocationRDD.rjoin(rightBoxes)(aggfunction1, aggfunction2)

    println("the outer table size: " + rightBoxes.count())
    println("the inner table size: " + leftpoints.count())
    val tuples = joinresultRdd.map { case (b, v) => (1, v) }.reduceByKey { case (a, b) => {
      a + b
    }
    }.map { case (a, b) => b }.collect()

    println("global index: " + Util.localIndex + " ; local index: " + Util.localIndex)
    println("query results size: " + tuples(0))
    println("spatial range join time: " + (System.currentTimeMillis - b1) + " (ms)")

    spark.stop()

  }

}

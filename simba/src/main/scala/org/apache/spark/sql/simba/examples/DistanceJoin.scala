package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.SimbaSession

object DistanceJoin {

  case class PointData(x: Double, y: Double)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession.builder().appName("Simba Distance Join").config("simba.join.partitions", "1024").config("simba.index.partitions", "1024").getOrCreate()

    val pointPath = args(0)
    val storePath = args(1)
    val distance = args(2).toDouble

    val (totalTime, joinTime, pairNum) = distanceJoin(pointPath, distance, simbaSession)
    val statistics = s"total_time:$totalTime;join_time:$joinTime;pair_num:$pairNum"
    println(statistics + "*********************")
    simbaSession.sparkContext.parallelize(Seq(statistics)).repartition(1)
      .saveAsTextFile(storePath)

    simbaSession.stop()
  }

  def distanceJoin(pointPath: String, distance: Double, simba: SimbaSession): (Long, Long, Long) = {

    import simba.implicits._
    import simba.simbaImplicits._

    var t0 = 0L
    var t1 = 0L
    t0 = System.nanoTime()
    var DS1 = simba.read.textFile(pointPath).map { line =>
      val parts = line.split(",")
      val longitude = parts(0).toDouble
      val latitude = parts(1).toDouble
      PointData(longitude, latitude)
    }.repartition(1024).toDF()
    var count1 = DS1.count()
    t1 = System.nanoTime()
    var left_time = (t1 - t0) / 1E9
    println("Left Read time: " + left_time + " seconds")

    var DS2 = simba.read.textFile(pointPath).map { line =>
      val parts = line.split(",")
      val longitude = parts(0).toDouble
      val latitude = parts(1).toDouble
      PointData(longitude, latitude)
    }.repartition(1024).toDF()
    count1 = DS2.count()
    t1 = System.nanoTime()
    var right_time = (t1 - t0) / 1E9
    println("Right Read time: " + right_time + " seconds")

    // Embed timers in DJSpark for partitioning time and indexing time
    t0 = System.nanoTime()
    val count = DS1.distanceJoin(DS2, Array("x", "y"), Array("x", "y"), distance).count()
    t1 = System.nanoTime()
    val runTime = (t1 - t0) / 1E9
    val totalTime = left_time + right_time + runTime

    (totalTime.toLong, runTime.toLong, count)
  }
}

package org.apache.spark.sql.simba.examples

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.spatial.Point

object Querier {

  case class PointData(p: Point, other: String)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession
      .builder()
      //.master("local[*]")
      .appName("SpatialClassInference")
      .config("simba.index.partitions", "100")
      .config("spark.driver.maxResultSize", "5g")
      .getOrCreate()

    import simbaSession.implicits._
    import simbaSession.simbaImplicits._
    //load
    val inputFile = args(0)
    val multiple = args(1).toInt
    val builder = new StringBuilder
    for (i <- 1 to multiple) {
      builder.append(inputFile).append("/").append(i).append(",")
    }
    builder.deleteCharAt(builder.length - 1)
    val points = simbaSession.sparkContext.textFile(builder.toString()).map(x => {
      val fields = x.split(",")
      PointData(Point(Array(fields(3).toDouble, fields(4).toDouble)), x)
    }).toDS

    //index
    var indexTime = System.currentTimeMillis()
    points.createOrReplaceTempView("view1")
    simbaSession.indexTable("view1", RTreeType, "rtree_index", Array("p"))
    simbaSession.showIndex("view1")
    indexTime = System.currentTimeMillis() - indexTime

    val res = simbaSession.sql("SELECT * FROM view1")
    //knn
    val pointList = buildPointList
    val k = args(2).toInt
    val knnNumberList = new util.ArrayList[java.lang.Long](pointList.length)
    val knnTimeList = new util.ArrayList[java.lang.Long](pointList.length)
    for (queryPointStr <- pointList) {
      val queryPoint = queryPointStr.split(",")
      val knnTime = System.currentTimeMillis()
      val knnRes = res.knn("p", Array(queryPoint(0).toDouble, queryPoint(1).toDouble), k) //20.0,110.0
      knnNumberList.add(knnRes.count())
      knnTimeList.add(System.currentTimeMillis() - knnTime)
    }

    //spatial
    val edgeLength = args(3).toInt
    val boxList = buildEnvelop(edgeLength, 10)
    val spatialNumberList = new util.ArrayList[java.lang.Long](boxList.length)
    val spatialTimeList = new util.ArrayList[java.lang.Long](boxList.length)
    for (boxStr <- boxList) {
      val mbr = boxStr.split(",")
      var spatialTime = System.currentTimeMillis()
      val spatialRes = res.range("p", Array(mbr(0).toDouble, mbr(1).toDouble), Array(mbr(2).toDouble, mbr(3).toDouble)) //20.0,105.0,25.0,115.0
      spatialNumberList.add(spatialRes.count())
      spatialTimeList.add(System.currentTimeMillis() - spatialTime)
    }


    val outputLocation = args(4)
    val path = new Path(outputLocation)
    val fs = path.getFileSystem(new Configuration())
    val outputStream = fs.append(path)
    outputStream.writeBytes(indexTime + "\n")
    outputStream.writeBytes(StatisticsUtil.median(knnTimeList) + "\t" + StatisticsUtil.max(knnTimeList) + "\t" + StatisticsUtil.min(knnTimeList) + "\t" + StatisticsUtil.average(knnTimeList) + "\t" + StatisticsUtil.average(knnNumberList) + "\n")
    outputStream.writeBytes(StatisticsUtil.median(spatialTimeList) + "\t" + StatisticsUtil.max(spatialTimeList) + "\t" + StatisticsUtil.min(spatialTimeList) + "\t" + StatisticsUtil.average(spatialTimeList) + "\t" + StatisticsUtil.average(spatialNumberList) + "\n\n")


    outputStream.close()
    fs.close()
    simbaSession.stop()
  }

  def buildPointList: Array[String] = {
    val pointListStr = "22.63,113.84;22.67,113.80;22.49,113.90;22.62,113.88;22.46,114.04"
    return pointListStr.split(";")
  }

  def buildEnvelop(edgeLength: Int, num: Int): List[String] = {
    val minLng = 113.00
    val minLat = 22.40
    val maxLng = 114.00
    val maxLat = 23.40
    val deltaLng = maxLng - minLng
    val deltaLat = maxLat - minLat
    val interal = 1.0 / num
    var envelopeList = List[String]()
    var i = 0
    while (i < num) {
      val mbrMinLng = minLng + deltaLng * interal * i
      val mbrMinLat = minLat + deltaLat * interal * i
      val mbrMaxLng = mbrMinLng + 0.009 * edgeLength
      val mbrMaxLat = mbrMinLat + 0.009 * edgeLength
      val mbrString: String = mbrMinLat + "," + mbrMinLng + "," + mbrMaxLat + "," + mbrMaxLng
      envelopeList = mbrString :: envelopeList

      i += 1
    }
    envelopeList
  }
}

package cs.purdue.edu

import java.util

import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

object Querier {
  def main(args: Array[String]): Unit = {
    //初始化SparkContext
    val conf = new SparkConf()
      .set("spark.driver.maxResultSize", "5g")
      .setAppName("LocationSpark")
    //.setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    //load
    val inputBaseLocation = args(0)
    val multiple = args(1).toInt
    val builder = new StringBuilder
    for (i <- 1 to multiple) {
      builder.append(inputBaseLocation).append("/").append(i).append(",")
    }
    builder.deleteCharAt(builder.length - 1)
    val locationRDD = sparkContext.textFile(builder.toString()).map {
      line =>
        val arry = line.split(",")
        try {
          (Point(arry(4).toFloat, arry(3).toFloat), line)
        } catch {
          case e: Exception =>
          //println("input format error")
        }
    }.map {
      case (point: Point, v) => (point, v)
      case () => null
      case _ => null
    }.filter(_ != null)

    //index
    var indexTime = System.currentTimeMillis()
    val indexedRDD = SpatialRDD(locationRDD).cache()
    indexTime = System.currentTimeMillis() - indexTime

    //knn
    val pointList = buildPointList //Point(110.389193f, 21.226881f)
    val k = args(2).toInt
    val knnNumberList = new util.ArrayList[java.lang.Long](pointList.size)
    val knnTimeList = new util.ArrayList[java.lang.Long](pointList.size)
    for (pointStr <- pointList) {
      val point = pointStr.split(",")
      val queryPoint = Point(point(0).toFloat, point(1).toFloat)
      val time = System.currentTimeMillis()
      val result = indexedRDD.knnFilter(queryPoint, k, id => true)
      knnNumberList.add(result.size.toLong)
      knnTimeList.add(System.currentTimeMillis() - time)
    }

    //spatial
    val edgeLength = args(3).toInt
    val boxList = buildEnvelop(edgeLength, 1)
    val spatialNumberList = new util.ArrayList[java.lang.Long](boxList.length)
    val spatialTimeList = new util.ArrayList[java.lang.Long](boxList.length)
    for (boxStr <- boxList) {
      val box = boxStr.split(",")
      val searchBox = Box(box(0).toFloat, box(1).toFloat, box(2).toFloat, box(3).toFloat)
      val time = System.currentTimeMillis()
      val result = indexedRDD.rangeFilter(searchBox, id => true)
      spatialNumberList.add(result.size.toLong)
      spatialTimeList.add(System.currentTimeMillis() - time)
    }


    //存储查询结果
    val outputLocation = args(4)
    val path = new Path(outputLocation)
    val fs = path.getFileSystem(new Configuration())
    val outputStream = fs.append(path)
    outputStream.writeBytes(indexTime + "\n")
    outputStream.writeBytes(StatisticsUtil.median(knnTimeList) + "\t" + StatisticsUtil.max(knnTimeList) + "\t" + StatisticsUtil.min(knnTimeList) + "\t" + StatisticsUtil.average(knnTimeList) + "\t" + StatisticsUtil.average(knnNumberList) + "\n")
    outputStream.writeBytes(StatisticsUtil.median(spatialTimeList) + "\t" + StatisticsUtil.max(spatialTimeList) + "\t" + StatisticsUtil.min(spatialTimeList) + "\t" + StatisticsUtil.average(spatialTimeList) + "\t" + StatisticsUtil.average(spatialNumberList) + "\n\n")

    //释放资源
    outputStream.close()
    fs.close()
    sparkContext.stop()
  }

  def buildPointList: Array[String] = {
    val pointListStr = "113.84f,22.63f"; //;113.80f,22.67f;113.90f,22.49f"
    return pointListStr.split(";")
  }

  private def buildEnvelop(edgeLength: Int, num: Int): List[String] = {
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
      val mbrString: String = mbrMinLat + "," + mbrMaxLat + "," + mbrMinLng + "," + mbrMaxLng
      envelopeList = mbrString :: envelopeList

      i += 1
    }
    envelopeList
  }
}

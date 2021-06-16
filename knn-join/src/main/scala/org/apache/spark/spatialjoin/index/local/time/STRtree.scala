package org.apache.spark.spatialjoin.index.local.time

import java.sql.Timestamp

import org.apache.spark.spatialjoin.extractor.STExtractor
import org.apache.spark.spatialjoin.index.local.rtree3d.STRTreeIndex
import org.locationtech.jts.geom.Geometry

/**
  * @author wangrubin3
  **/
class STRtree[T](extractor: STExtractor[T], k: Int, binNum: Int) extends Serializable {
  private val rtree = new STRTreeIndex[T](extractor)
  private var isBuilt = false
  private var empty = true

  def build(rows: Array[T]): Unit = {
    rows.foreach(row => rtree.insert(row))
    rtree.build()
    empty = rows.isEmpty
    isBuilt = true
  }

  def nearestNeighbour(queryGeom: Geometry,
                       queryStart: Timestamp,
                       queryEnd: Timestamp,
                       k: Int,
                       isValid: T => Boolean,
                       maxDistance: Double = Double.MaxValue): Array[(T, Double)] = {
    rtree.nearestNeighbour(queryGeom, queryStart, queryEnd, k, isValid, maxDistance)
  }

  def isEmpty: Boolean = this.empty
}

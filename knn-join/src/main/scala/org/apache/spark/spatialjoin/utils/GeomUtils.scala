package org.apache.spark.spatialjoin.utils

import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory, Point}
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}

/**
  * @author wangrubin3
  **/
object GeomUtils {
  val defaultFactory = new GeometryFactory()

  def getCentre(env: Envelope): Point = {
    defaultFactory.createPoint(env.centre())
  }
}

object WKBUtils {
  private[this] val readerPool = new ThreadLocal[WKBReader] {
    override def initialValue: WKBReader = new WKBReader
  }
  private[this] val writerPool = new ThreadLocal[WKBWriter] {
    override def initialValue: WKBWriter = new WKBWriter
  }

  def read(b: Array[Byte]): Geometry = readerPool.get.read(b)

  def write(g: Geometry): Array[Byte] = writerPool.get.write(g)
}

object WKTUtils {
  private[this] val readerPool = new ThreadLocal[WKTReader] {
    override def initialValue: WKTReader = new WKTReader
  }
  private[this] val writerPool = new ThreadLocal[WKTWriter] {
    override def initialValue: WKTWriter = new WKTWriter
  }

  def read(s: String): Geometry = readerPool.get.read(s)

  def write(g: Geometry): String = writerPool.get.write(g)
}

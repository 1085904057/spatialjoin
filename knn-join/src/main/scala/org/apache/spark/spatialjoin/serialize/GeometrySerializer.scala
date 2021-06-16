package org.apache.spark.spatialjoin.serialize

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.spatialjoin.utils.WKBUtils
import org.locationtech.jts.geom.Geometry

/**
  * @author wangrubin3
  **/
class GeometrySerializer extends Serializer[Geometry] {
  override def write(kryo: Kryo, output: Output, geom: Geometry): Unit = {
    val spatialLineBytes: Array[Byte] = WKBUtils.write(geom)
    output.writeInt(spatialLineBytes.length)
    output.writeBytes(spatialLineBytes)
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[Geometry]): Geometry = {
    val length = input.readInt()
    WKBUtils.read(input.readBytes(length))
  }
}

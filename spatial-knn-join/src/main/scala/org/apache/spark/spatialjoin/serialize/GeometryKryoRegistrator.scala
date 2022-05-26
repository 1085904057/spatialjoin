package org.apache.spark.spatialjoin.serialize

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.locationtech.jts.geom._

/**
 * @author wangrubin
 */
class GeometryKryoRegistrator  extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    val geometrySerializer = new GeometrySerializer

    kryo.register(classOf[Point], geometrySerializer)
    kryo.register(classOf[LineString], geometrySerializer)
    kryo.register(classOf[Polygon], geometrySerializer)
    kryo.register(classOf[MultiPoint], geometrySerializer)
    kryo.register(classOf[MultiLineString], geometrySerializer)
    kryo.register(classOf[MultiPolygon], geometrySerializer)
    kryo.register(classOf[Geometry], geometrySerializer)
  }
}

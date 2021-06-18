
package cs.purdue.edu.spatialindex.rtree

import scala.math.{max, min, sqrt}
import java.lang.Float.{isInfinite, isNaN}

import cs.purdue.edu.just.examples.TimeRange

/**
 * Geometry represents a box (or point).
 *
 * (x1, y1) is the lower left point and (x2, y2) is upper right.
 *
 * So Box(1, 2, 5, 4) is at (1, 2) and is 4 wide and 2 high.
 * Points have no width or height, so x2/y2 are equal to x/y.
 */
sealed trait Geom {
  def x: Float
  def y: Float
  def x2: Float
  def y2: Float
  def width: Float = x2 - x
  def height: Float = y2 - y

  def area: Float = width * height

  /**
   * Distance between this geometry and the given point.
   *
   * The distance is measured in terms of the closest point in the
   * geometry. For points this is obvious (there is only one point to
   * use). For boxes, this means that points contained within the box
   * (or on the perimeter) have a distance of zero.
   */
  def distance(pt: Point): Double =
    sqrt(distanceSquared(pt))

  /**
   * Distance between this geometry and the given point, squared.
   *
   * See distance() for how to interpret the distance metric. This
   * method is a bit faster than distance(), and is exposed for cases
   * where we want to compare two distances (without caring about the
   * actual distance value).
   */
  def distanceSquared(pt: Point): Double = {
    val dx = (if (pt.x < x) x - pt.x else if (pt.x < x2) 0F else pt.x - x2).toDouble
    val dy = (if (pt.y < y) y - pt.y else if (pt.y < y2) 0F else pt.y - y2).toDouble
    dx * dx + dy * dy
  }

  /**
   * Return whether all the bounds of the geometry are finite.
   *
   * "Finite" means all values except NaN and infinities.
   */
  def isFinite: Boolean =
    !(isNaN(x) || isInfinite(x) ||
      isNaN(y) || isInfinite(y) ||
      isNaN(x2) || isInfinite(x2) ||
      isNaN(y2) || isInfinite(y2))

  /**
   * Convert this Geom to a Box.
   *
   * For Boxes this is a no-op. For points, it constructs a box with
   * width and height of zero.
   */
  def toBox: Box = Box(x, y, x2, y2)

  /**
   * Get the lower-left (southwest) bound of the geometry.
   */
  def lowerLeft: Point = Point(x, y)

  /**
   * Get the upper-right (northeast) bound of the geometry.
   */
  def upperRight: Point = Point(x2, y2)

  /**
   * Returns whether this geometry contains the other.
   *
   * Containment includes the border, so points "on the edge" count as
   * contained.
   */
  def contains(geom: Geom): Boolean =
    x <= geom.x && geom.x2 <= x2 && y <= geom.y && geom.y2 <= y2

  def ==(geom: Geom):Boolean=
    x == geom.x && geom.x2 == x2 && y == geom.y && geom.y2 == y2
  /**
   * Returns whether this geometry intersects with the other.
   *
   * Intersection includes the border, so points "on the edge" count
   * as intersecting.
   */
  def intersects(geom: Geom): Boolean =
    x <= geom.x2 && geom.x <= x2 && y <= geom.y2 && geom.y <= y2

  /**
   * Returns whether this geometry wraps the other.
   *
   * This is the same thing as containment, but it excludes the
   * border. Points can never wrap anything, and boxes can only wrap
   * geometries with less area than they have.
   */
  def wraps(geom: Geom): Boolean =
    x < geom.x && geom.x2 < x2 && y < geom.y && geom.y2 < y2

  /**
   * Construct a Box that contains this geometry and the other.
   *
   * This will be the smallest possible box. The result of this method
   * is guaranteed to contain() both geometries.
   */
  def expand(geom: Geom): Box = {
    val px1 = min(x, geom.x)
    val py1 = min(y, geom.y)
    val px2 = max(x2, geom.x2)
    val py2 = max(y2, geom.y2)
    Box(px1, py1, px2, py2)
  }

  override def hashCode:Int = {
    /*import java.security.MessageDigest
    val digest = MessageDigest.getInstance("MD5")
    val b1=this.x.toByte
    val b2=this.y2.toByte
    val b3=this.x2.toByte
    val b4=this.y.toByte
    val bytes=Array(b1,b2,b3,b4)
    val md5hash1 = digest.digest(bytes).hashCode()*/

    val t1=this.x*this.y
    val b2=this.y2*this.x2

    val ret2=((this.x+this.y+this.x2+this.y2+t1*1000+b2*100000)%1000000).toInt

    return ret2
  }
  /**
   * Return the given geometry's area outside this geometry.
   *
   * This is equivalent to the area that would be added by expand().
   */
  def expandArea(geom: Geom): Float = {
    val px1 = min(x, geom.x)
    val py1 = min(y, geom.y)
    val px2 = max(x2, geom.x2)
    val py2 = max(y2, geom.y2)
    val a = (py2 - py1) * (px2 - px1)
    a - area
  }


}

case class Point(x: Float, y: Float, tr: TimeRange = null, globalTimeRange: Long = 0, k: Int = 5, threshold: Long = 0) extends Geom {
  def x2: Float = x
  def y2: Float = y

  def getDynamicK(): Int =
    if(tr.startTime != tr.endTime) {
      (k * (globalTimeRange / tr.getDeltaTime())).toInt
    } else (k * (globalTimeRange / tr.getDeltaTime(threshold))).toInt

  def isIntersects(outer: TimeRange): Boolean = {
    val extendStartTime = tr.startTime.getTime - threshold
    val extendEndTime = tr.endTime.getTime + threshold
    !(outer.startTime.getTime > extendEndTime || outer.endTime.getTime < extendStartTime)
  }

  override def width: Float = 0F
  override def height: Float = 0F
  override def area: Float = 0F
  override def lowerLeft: Point = this
  override def upperRight: Point = this

  override def distanceSquared(pt: Point): Double = {
    val dx = (pt.x - x).toDouble
    val dy = (pt.y - y).toDouble
    dx * dx + dy * dy
  }

  override def isFinite: Boolean =
    !(isNaN(x) || isInfinite(x) || isNaN(y) || isInfinite(y))

  override def wraps(geom: Geom): Boolean = false

  override def toString():String={

    "("+x+","+y+")"
  }
}

case class Box(x: Float, y: Float, x2: Float, y2: Float) extends Geom {
  override def toBox: Box = this

  override def toString:String={x+","+y+"; "+x2+","+y2}

  var knndistancebound:Double=Double.MaxValue

  def intersectionarea(other:Box):Double=
  {
    Math.abs(
      Math.max(0, (Math.min(this.y2,other.y2)-Math.max(this.y,other.y)))
    *
      Math.max(0, (Math.min(this.x2,other.x2)-Math.max(this.x,other.x)))
    )

  }

  def intesectBox(other:Box):Box={

    Box(Math.max(this.x,other.x),Math.max(this.y,other.y), Math.min(this.x2,other.x2),Math.min(this.y2,other.y2))

  }

  def mindistance(other:Box):Double=
  {
    if(this.intersects(other))
    {
      0
    }else
    {
      Math.min(
        Math.min(Math.abs(this.x2-other.x),Math.abs(this.x-other.x2)),
        Math.min(Math.abs(this.y2-other.y),Math.abs(this.y-other.y2))
      )
    }
  }

}

object Box {

  /**
   * This is an "inside-out" box that we use as a good starting
   * value. The nice thing about this, unlike Box(0,0,0,0), is that
   * when merging with another box we don't include an artifical
   * "empty" point.
   */
  val empty: Box = {
    val s = Math.sqrt(Float.MaxValue).toFloat
    val t = s + -2.0F * s
    Box(s, s, t, t)
  }
}

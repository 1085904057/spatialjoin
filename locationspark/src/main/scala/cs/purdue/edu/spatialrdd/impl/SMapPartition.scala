package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.spatialindex.quatree.SBQTree
import cs.purdue.edu.spatialindex.rtree.Entry
import cs.purdue.edu.spatialrdd.SpatialRDDPartition
import cs.purdue.edu.spatialindex.rtree._
import org.apache.spark.internal.Logging

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by merlin on 11/22/15.
  */

class SMapPartition[K, V]
(protected val data: HashMap[K, V])
(
  override implicit val kTag: ClassTag[K],
  override implicit val vTag: ClassTag[V]
)
  extends SpatialRDDPartition[K, V] with Logging {

  //val Quadfilter=new SBQTree(1000)
  //qtree.trainSBfilter(datapoint)

  override def isDefined(k: K): Boolean = {
    data.contains(k)
  }

  override def size: Long = {
    data.size
  }

  override def iterator: Iterator[(K, V)] = {
    data.toIterator
  }

  /** Return the value for the given key. */
  override def apply(k: K): Option[V] = {
    this.data.get(k)
  }

  /**
    * range search and find points inside the box, and each element meet the condition, and return a iterator,
    * and this iterator can be used for other RDD
    */
  override def filter[U](box: U, z: (Entry[V]) => Boolean): Iterator[(K, V)] = {

    //go through the hash table for searching

    data.filter {
      case (p: Point, v) => {
        box match {
          case b: Box =>
            b.contains(p) && z(Entry(p, v))
        }
      }
    }.toIterator

  }

  /** Deletes the specified spatial entry elements. Returns a new IndexedRDDPartition that reflects the deletions. */
  override def delete(ks: Iterator[Entry[V]]): SpatialRDDPartition[K, V] = {

    var ret = data

    ks.foreach {
      element =>
        if (data.contains(element.geom.asInstanceOf[K])) {
          ret = ret.-(element.geom.asInstanceOf[K])
        }
    }
    new SMapPartition(ret)
  }

  /**
    * Gets the values corresponding to the specified keys, if any. those keys can be the two dimensional object
    */
  def multiget(ks: Iterator[K]): Iterator[(K, V)] = {

    ks.flatMap { k => this.data.get(k).map(v => (k, v)) }
  }

  /**
    * spatial range join operation
    * the other rdd is query rdd.
    * the key is the location of the range query box, and value is the range query box
    * the f function apply to the value of the filter condition
    */
  def sjoin[U: ClassTag]
  (other: SpatialRDDPartition[K, U])
  (f: (K, V) => V): SpatialRDDPartition[K, V] = sjoin(other.iterator)(f)


  /**
    * the iterator is a key and value paris,
    * key is partition location of the box, the value is the queried box
    * the key is the location of the range query box, and value is the range query box
    * the f function apply to the value of the filter condition
    */
  def sjoin[U: ClassTag]
  (other: Iterator[(K, U)])
  (f: (K, V) => V): SpatialRDDPartition[K, V] = {

    var ret = new HashMap[K, V]

    other.foreach {
      case (pid, b: Box) =>
        //if(this.Quadfilter.queryBox(b))
      {
        this.data.foreach {
          case (p, v) =>

            if (b.contains(p.asInstanceOf[Geom]) && (!ret.contains(p))) {
              ret = ret + (p -> f(p, v))
            }
        }
      }

    }

    new SMapPartition(ret)
  }

  override def rjoin[U: ClassTag, U2: ClassTag]
  (other: SpatialRDDPartition[K, U])
  (f: (Iterator[(K, V)]) => U2,
   f2: (U2, U2) => U2): Iterator[(U, U2)] = rjoin(other.iterator)(f, f2)

  def rjoin[U: ClassTag, U2: ClassTag]
  (other: Iterator[(K, U)])
  (f: (Iterator[(K, V)]) => U2,
   f2: (U2, U2) => U2): Iterator[(U, U2)] = {

    val buf = mutable.HashMap.empty[Geom, ArrayBuffer[(K, V)]]

    def updatehashmap(key: Geom, v2: V, k2: K) = {
      try {
        if (buf.contains(key)) {
          val tmp1 = buf.get(key).get
          tmp1.append(k2 -> v2)
          buf.put(key, tmp1)
        } else {
          val tmp1 = new ArrayBuffer[(K, V)]
          tmp1.append((k2 -> v2))
          buf.put(key, tmp1)
        }

      } catch {
        case e: Exception =>
          println("out of memory for appending new value to the sjoin")
      }
    }

    other.foreach {
      case (pid, b: Box) => {
        this.data.foreach {
          case (p, v) =>

            if (b.contains(p.asInstanceOf[Geom])) {
              updatehashmap(b, v, p)
            }
        }
      }

    }

    buf.toIterator.map {
      case (g, array) =>
        val agg = f(array.toIterator)
        array.clear()
        (g.asInstanceOf[U], agg)
    }

  }

  override def knnfilter[U](entry: U, k: Int, z: Entry[V] => Boolean): Iterator[(K, V, Double)] = {
    entry match {
      case e: Point => {
        val ret = this.data.map {
          case (k, v) =>
            (k, v, entry.asInstanceOf[Entry[V]].geom.distanceSquared(k.asInstanceOf[Point]))
        }

        ret.toList.sortWith(_._3 < _._3).take(k).toIterator

      }
    }
  }

  /**
    * Updates the keys in `kvs` to their corresponding values generated by running `f` on old and new
    * values, if an old value exists, or `z` otherwise. Returns a new IndexedRDDPartition that
    * reflects the modification.
    */
  override def multiput[U](kvs: Iterator[(K, U)],
                           z: (K, U) => V,
                           f: (K, V, U) => V):
  SpatialRDDPartition[K, V] = {

    var newMap = this.data

    for (ku <- kvs) {

      val oldV = newMap.get(ku._1).get

      val newV = if (oldV == null) z(ku._1, ku._2) else f(ku._1, oldV, ku._2)

      val newEntry = Util.toEntry(ku._1, newV).asInstanceOf[K]

      newMap = newMap + (newEntry -> newV)

    }

    new SMapPartition(newMap)
  }

  override def knnjoin_[U: ClassTag]
  (other: SpatialRDDPartition[K, U], knn: Int, f1: (K) => Boolean,
   f2: (V) => Boolean)
  : Iterator[(K, Double, Iterator[(K, V)])] = knnjoin_(other.iterator, knn, f1, f2)

  def knnjoin_[U: ClassTag]
  (other: Iterator[(K, U)], knn: Int, f1: (K) => Boolean,
   f2: (V) => Boolean): Iterator[(K, Double, Iterator[(K, V)])] = {
    val newMap = this.data

    val buff = ArrayBuffer.empty[(K, Double, Iterator[(K, V)])]

    //nest loop knn search
    other.foreach {
      case (p1: Point, k: Int) =>
        val ret = this.data.map {
          case (point: Point, v) =>
            (point, v, p1.distanceSquared(point))
        }

        val tmp2 = ret.toList.sortWith(_._3 < _._3).take(k)
        val distance = tmp2(k - 1)._3

        val tmp3 = tmp2.map { e => (e._1.asInstanceOf[K], e._2) }.toIterator

        buff.append((p1.asInstanceOf[K], distance, tmp3))
    }

    buff.toIterator
  }

  /**
    * @todo add this function in future
    * @param other
    * @return
    */
  override def rkjoin(other: Iterator[(K, (K, Iterator[(K, V)], Box))], f1: (K) => Boolean,
                      f2: (V) => Boolean, k: Int): Iterator[(K, Iterable[(K, V)])] = {

    other.map {
      case (locationpoint, (querypoint, itr, box))
      =>
        (querypoint, itr.toIterable)
    }
  }

}

private[spatialrdd] object SMapPartition {

  def apply[K: ClassTag, V: ClassTag]
  (iter: Iterator[(K, V)]) =
    apply[K, V, V](iter, (id, a) => a, (id, a, b) => b)

  def apply[K: ClassTag, U: ClassTag, V: ClassTag]
  (iter: Iterator[(K, V)], z: (K, U) => V, f: (K, V, U) => V)
  : SpatialRDDPartition[K, V] = {

    var map = new HashMap[K, V]

    iter.foreach {
      case (k, v) => map = map + (k -> v)
    }

    val smp = new SMapPartition(map)
    //smp.Quadfilter.trainSBfilter(iter.map{case(k,v)=>k.asInstanceOf[Geom]})

    smp
  }

}
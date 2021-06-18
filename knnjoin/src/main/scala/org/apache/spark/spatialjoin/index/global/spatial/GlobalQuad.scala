package org.apache.spark.spatialjoin.index.global.spatial

import java.util.{Comparator, PriorityQueue}

import org.locationtech.jts.geom.{Envelope, Point}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
  * @author wangrubin3
  **/
class GlobalQuad(spatialBound: Envelope) extends GlobalIndex {
  private val root = new QuadNode(spatialBound)
  private var leafNodes: Array[QuadNode] = _

  def build(samples: Array[Envelope], sampleRate: Double, beta: Int, k: Int): Unit = {
    val comparator = new Comparator[(QuadNode, Array[Envelope])] {
      override def compare(o1: (QuadNode, Array[Envelope]), o2: (QuadNode, Array[Envelope])): Int = {
        0 - Integer.compare(o1._2.length, o2._2.length)
      }
    }
    val maxNumPerPartition = Math.max(samples.length / beta, Math.ceil(4 * sampleRate * k).toInt)
    val queue = new PriorityQueue[(QuadNode, Array[Envelope])](comparator)
    queue.add((root, samples))
    while (queue.size() < beta) {
      if (queue.peek()._2.length < maxNumPerPartition) {
        this.leafNodes = queue.asScala.map(_._1).toArray
        return
      } else {
        val (maxNode, maxSamples) = queue.poll()
        maxNode.split(maxSamples, queue)
      }
    }
    this.leafNodes = queue.asScala.map(_._1).toArray
  }

  override def findNearestId(queryCentre: Point,
                             timeFilter: GlobalNode => Boolean): Int = {
    root.findNearestId(queryCentre, timeFilter)
  }

  override def findIntersectIds(queryEnv: Envelope,
                                idCollector: ArrayBuffer[Int]): Unit = {
    root.findIntersectIds(queryEnv, idCollector)
  }

  override def assignPartitionId(baseId: Int): Int = {
    for (i <- leafNodes.indices) {
      leafNodes(i).setPartitionId(baseId + i)
    }
    baseId + leafNodes.length
  }

  override def getLeafEnv(index: Int): Envelope = leafNodes(index).env
}

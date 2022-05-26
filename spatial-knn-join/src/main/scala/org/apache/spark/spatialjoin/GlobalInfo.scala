package org.apache.spark.spatialjoin

import org.locationtech.jts.geom.{Envelope, Geometry}

/**
 * @author wangrubin
 */
class GlobalInfo(val env: Envelope, var count: Long) extends Serializable {
  def add(geom: Geometry): GlobalInfo = {
    env.expandToInclude(geom.getEnvelopeInternal)
    count += 1
    this
  }

  def combine(other: GlobalInfo): GlobalInfo = {
    env.expandToInclude(other.env)
    count += other.count
    this
  }

  def getEnv: Envelope = {
    env
  }

  def getCount: Long = {
    count
  }
}
package org.apache.spark.spatialjoin.join

/**
  * @author wangrubin3
  **/
case class ApproximateContainer[T](duplicateKnn: Array[(T, Double)],
                                   partitionId: Int,
                                   expandDist: Double)

case class RowWithId[T](id: Long, row: T) extends Serializable {
  override def hashCode(): Int = this.id.hashCode()

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: RowWithId[T] =>
        this.id.equals(other.id)
      case _ => false
    }
  }
}
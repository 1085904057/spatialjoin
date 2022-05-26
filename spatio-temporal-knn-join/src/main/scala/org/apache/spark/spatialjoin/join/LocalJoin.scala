package org.apache.spark.spatialjoin.join

import java.sql.Timestamp

import org.apache.spark.spatialjoin.extractor.STExtractor
import org.apache.spark.spatialjoin.index.global.time.STBound
import org.apache.spark.spatialjoin.index.local.time.STRtree
import org.apache.spark.spatialjoin.utils.TimeUtils
import org.locationtech.jts.geom.Envelope

import scala.reflect.ClassTag

/**
  * @author wangrubin3
  **/
class LocalJoin[R: ClassTag, S: ClassTag](leftExtractor: STExtractor[R],
                                          rightExtractor: STExtractor[S],
                                          deltaMilli: Long,
                                          k: Int) extends Serializable {

  def firstRoundJoin(leftRows: Iterator[RowWithId[R]],
                     localIndex: STRtree[S],
                     partitionBound: STBound,
                     partitionId: Int): Iterator[(RowWithId[R], ApproximateContainer[S])] = {

    if (leftRows.isEmpty || localIndex.isEmpty) return Iterator.empty

    leftRows.map {
      case rowWithId@RowWithId(id, leftRow) =>
        if (id > 0) {
          val leftGeom = leftExtractor.geom(leftRow)
          val leftStartTime = leftExtractor.startTime(leftRow)
          val leftEndTime = leftExtractor.endTime(leftRow)
          val leftTimeRange@(eStart, eEnd) = TimeUtils.expandTimeRange((leftStartTime, leftEndTime), deltaMilli)
          val isValid = (rightRow: S) => {
            val rightTimeRange = (rightExtractor.startTime(rightRow), rightExtractor.endTime(rightRow))
            TimeUtils.isIntersects(leftTimeRange, rightTimeRange)
          }
          val candidatesWithDist = localIndex.nearestNeighbour(leftGeom, eStart, eEnd, k, isValid)
          assert(candidatesWithDist.length == k)
          val leftGeomEnv = leftGeom.getEnvelopeInternal
          val maxDist = candidatesWithDist.last._2
          leftGeomEnv.expandBy(maxDist)
          val duplicatedCandidates = candidatesWithDist.filter(rightRow => {
            val rightGeomEnv = rightExtractor.geom(rightRow._1).getEnvelopeInternal
            val rightTimeRange = (rightExtractor.startTime(rightRow._1), rightExtractor.endTime(rightRow._1))
            isReserve(partitionBound, leftGeomEnv, rightGeomEnv, leftTimeRange, rightTimeRange)
          })
          (rowWithId, new ApproximateContainer[S](duplicatedCandidates, partitionId, maxDist))
        } else {
          //compute in second round
          (rowWithId, new ApproximateContainer[S](Array.empty, partitionId, Double.MaxValue))
        }
    }
  }

  def secondRoundJoin(leftRows: Iterator[(RowWithId[R], ApproximateContainer[S])],
                      localIndex: STRtree[S],
                      partitionBound: STBound): Iterator[(RowWithId[R], Array[(S, Double)])] = {

    if (leftRows.isEmpty || localIndex.isEmpty) return Iterator.empty

    leftRows.map {
      case (rowWithId@RowWithId(_, leftRow), container) =>
        if (container.partitionId != -1) {
          (rowWithId, container.duplicateKnn)
        } else {
          val leftGeom = leftExtractor.geom(leftRow)
          val leftStartTime = leftExtractor.startTime(leftRow)
          val leftEndTime = leftExtractor.endTime(leftRow)
          val leftGeomEnv = leftGeom.getEnvelopeInternal
          leftGeomEnv.expandBy(container.expandDist)
          val leftTimeRange@(eStart, eEnd) = TimeUtils.expandTimeRange((leftStartTime, leftEndTime), deltaMilli)
          val isValid = (rightRow: S) => {
            val rightTimeRange = (rightExtractor.startTime(rightRow), rightExtractor.endTime(rightRow))
            TimeUtils.isIntersects(leftTimeRange, rightTimeRange)
          }
          val candidatesWithDist = localIndex.nearestNeighbour(leftGeom, eStart, eEnd, k, isValid, container.expandDist)
          val duplicatedCandidates = candidatesWithDist.filter(rightRow => {
            val rightGeomEnv = rightExtractor.geom(rightRow._1).getEnvelopeInternal
            val rightTimeRange = (rightExtractor.startTime(rightRow._1), rightExtractor.endTime(rightRow._1))
            isReserve(partitionBound, leftGeomEnv, rightGeomEnv, leftTimeRange, rightTimeRange)
          })
          (rowWithId, duplicatedCandidates)
        }
    }
  }

  def isReserve(partitionBound: STBound,
                leftGeomEnv: Envelope, rightGeomEnv: Envelope,
                leftTimeRange: (Timestamp, Timestamp),
                rightTimeRange: (Timestamp, Timestamp)): Boolean = {

    //time duplicate
    val timeReferPoint = TimeUtils.timeReferPoint(leftTimeRange, rightTimeRange)
    val isTimeSatisfied = timeReferPoint != null && partitionBound.contains(timeReferPoint)
    //spatial duplicate
    if (isTimeSatisfied) {
      val intersection = leftGeomEnv.intersection(rightGeomEnv)
      intersection.getWidth >= 0 &&
        partitionBound.contains(intersection.getMinX, intersection.getMinY)
    } else false
  }
}

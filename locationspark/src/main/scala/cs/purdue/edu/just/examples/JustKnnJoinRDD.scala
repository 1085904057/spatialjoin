package cs.purdue.edu.just.examples

import cs.purdue.edu.scheduler.skewAnalysis
import cs.purdue.edu.spatialindex.quatree.QtreeForPartion
import cs.purdue.edu.spatialindex.rtree.{Box, Geom, Point}
import cs.purdue.edu.spatialrdd.impl.{QtreePartitioner, QtreePartitionerBasedQueries, knnJoinRDD}
import cs.purdue.edu.spatialrdd.{SpatialRDD, SpatialRDDPartition}
import org.apache.spark.rdd.RDD

class JustKnnJoinRDD(datardd: SpatialRDD[Point, String],
                     queryrdd: RDD[Point],
                     knn: Int,
                     f1: Point => Boolean,
                     f2: String => Boolean) extends knnJoinRDD[Point, String](datardd, queryrdd, knn, f1, f2) {

  def rangebasedKnnjoin(timeDelta: Long): RDD[(Point, Iterator[(Point, String)])] = {
    val knn=this.knn
    //step1: partition the queryrdd if the partitioner of query and data rdd is different
    val tmpqueryrdd=queryrdd.map(key=>(key,knn))

    val partitionedRDD =tmpqueryrdd.partitionBy(datardd.partitioner.get)

    //localKnnJoinRDD with this format: RDD[p, iterator[(k,v)]]
    val localKnnJoinRDD = datardd.partitionsRDD.zipPartitions(partitionedRDD, true) {
      (thisIter, otherIter) =>
        val thisPart: SpatialRDDPartition[Point, String] = thisIter.next()
        thisPart.knnjoin_(otherIter, knn, f1,f2)
    }.cache()

    //localKnnJoinRDD.collect().foreach(println)

    def distancetoBox(point:Point,max:Double):Box=
    {
      val p=point.asInstanceOf[Point]
      Box((p.x-max).toFloat,(p.y-max).toFloat,(p.x+max).toFloat,(p.y+max).toFloat)
    }

    //step3: map the knn join to range join. execute the rjoin and return [q. iterator[(k,v)]]
    val pointboxrdd=localKnnJoinRDD.map{
      case(point, max,itr)=>
        (point,itr, distancetoBox(point,max))
    }


    val firstRoundKJOINwithBox=pointboxrdd.map
    {
      case(point,itr, box)=>
        this.datardd.partitioner.get match
        {
          case  qtreepartition:QtreePartitioner[_, _] =>
            (point, itr, qtreepartition.getPointsForSJoin(box),box)
        }
    }

    val correctKNN=firstRoundKJOINwithBox.filter{
      case(k,itr, hashset,box)=>
        hashset.size==1
    }.map
    {
      case(k,itr, hashset,box)=>
        (k,itr)
    }

    val nextRoundKNN=firstRoundKJOINwithBox.filter{
      case(k,itr,hashset,box)=>
        hashset.size>1
    }



    //option1: range search over overlap partitions
    def rangejoin():RDD[(Point,Iterator[(Point, String)])]=
    {
      //map this knnrdd to related partition rdd
      val pointstoPIDRDD=nextRoundKNN.flatMap {
        case(k,itr,hashset,box)=>
          hashset.map(p => (p.asInstanceOf[Point], (k,itr,box)))
      }

      /******************************************************************************/
      /******************************add scheduler to this part**********************/

      val stat=analysis(this.datardd,pointstoPIDRDD)

      val topKpartitions=skewAnalysis.findTopKSkewPartition(stat,5)

      val broadcastVar = this.datardd.context.broadcast(topKpartitions)

      //transform the skew and query rdd
      val skew_queryrdd = pointstoPIDRDD.mapPartitionsWithIndex{
        (pid,iter)=>
          broadcastVar.value.contains(pid) match
          {
            case true=> iter
            case false=> Iterator.empty
          }
      }

      val skew_datardd= this.datardd.mapPartitionsWithIndex(
        (pid,iter)=> broadcastVar.value.contains(pid) match
        {
          case true=>iter
          case false=>Iterator.empty
        },true
      )

      val nonskew_queryrdd = pointstoPIDRDD.mapPartitionsWithIndex{
        (pid,iter)=>
          broadcastVar.value.contains(pid) match
          {
            case false=> iter
            case true=> Iterator.empty
          }
      }

      val nonskew_datardd =this.datardd

      def getPartitionerbasedQuery(topKpartitions:Map[Int,Int], skewQuery:RDD[(Point,(Point,Iterator[(Point,String)],Box))]):
      QtreePartitionerBasedQueries[Int,QtreeForPartion] =
      {
        //default nubmer of queries
        val samplequeries=skewQuery.sample(false,0.02f).map{case(point, (pt,itr,box))=>box}.distinct().collect()

        //get the quadtree partionner from this data rdd, and colone that quadtree
        val qtreepartition=new QtreeForPartion(100)
        this.datardd.partitioner.getOrElse(None) match {
          case qtreepter: QtreePartitioner[_,_] =>
            val newrootnode=qtreepter.quadtree.coloneTree()
            //qtreepter.quadtree.printTreeStructure()
            qtreepartition.root=newrootnode
        }

        //run those queries over the old data partitioner
        samplequeries.foreach
        {
          case box:Box=>qtreepartition.visitleafForBox(box)
        }

        val partitionnumberfromQueries= qtreepartition.computePIDBasedQueries(topKpartitions)

        new QtreePartitionerBasedQueries(partitionnumberfromQueries,qtreepartition)

      }

      val newpartitioner=getPartitionerbasedQuery(topKpartitions,skew_queryrdd)
      val skewindexrdd=SpatialRDD.buildSRDDwithgivenPartitioner(skew_datardd,newpartitioner)

      //execute the join between the skew index rdd with skew_queryrdd
      val repartitionSkewQueryRDD = skew_queryrdd.partitionBy(newpartitioner)
      val skewresultRDD = skewindexrdd.partitionsRDD.zipPartitions(repartitionSkewQueryRDD, true)
      {
        (thisIter, otherIter) =>
          val thisPart: SpatialRDDPartition[Point, String] = thisIter.next()
          thisPart.rkjoin(otherIter,f1,f2,knn)
      }

      //execute the join between the non skew index rdd with non skew_queryrdd
      val nonskewresultRDD = nonskew_datardd.partitionsRDD.zipPartitions(nonskew_queryrdd, true)
      {
        (thisIter, otherIter) =>
          val thisPart = thisIter.next()
          thisPart.rkjoin(otherIter,f1,f2,knn)
      }



      /******************************************************************************/
      /***************************finish the skew part ******************************/
      /******************************************************************************/

      val joinResultRDD=skewresultRDD.union(nonskewresultRDD)

      /******************************************************************************/


      joinResultRDD.reduceByKey((itr1,itr2)=>itr1++itr2,correctKNN.partitions.size/2).map
      {
        case(querypoint,itr)=>

          val tmpit=itr.map
          {
            case(location,value)=>
              (querypoint.asInstanceOf[Geom].distance(location.asInstanceOf[Point]),location,value)
          }.toArray.sortBy(_._1).distinct.slice(0,knn).map
          {
            case(distance,p,value)=>(p,value)
          }.toIterator

          (querypoint,tmpit)
      }
    }

    val rangejoinforknnRDD=rangejoin()

    rangejoinforknnRDD.union(correctKNN)
  }

}
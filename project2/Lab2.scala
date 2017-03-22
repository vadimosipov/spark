package org.test

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import scala.concurrent.ExecutionContext.Implicits._

object Lab2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myapp")
                              .setMaster("local[3]")
                              .set("spark.scheduler.allocation.file", "myschedulerpool2.xml")
    val sc = new SparkContext(conf)
    sc.setLocalProperty("spark.scheduler.pool", "mypool")
    val path = "/home/vadim/projects/test/data"

    val input1 = sc.textFile(path + "/trips/*")
    val header1 = input1.first()
    val trips = input1.filter(_ != header1).map(_.split(","))

    val input2 = sc.textFile(path + "/stations/*")
    val header2 = input2.first()
    println("-------------------")
    var start = System.currentTimeMillis()
    val stations = input2.filter(_ != header2)
    val stations2 = stations.map(_.split(","))
    val stations3 = stations2.keyBy(_(0).toInt)
    val stations4 = stations3.partitionBy(new HashPartitioner(trips.partitions.length))

    val trips2 = trips.keyBy(_(4).toInt)
    val startTrips = stations4.join(trips2)
    val endTrips = stations4.join(trips2)

    val fun1 = startTrips.countAsync()
    val fun2 = endTrips.countAsync()

    fun1.onComplete(t => {
      val end = System.currentTimeMillis()
      println("startTrips: " + t.getOrElse(0))
      println("without partitionBy: " + (end - start))
    })
    fun2.onComplete(t => {
      val end2 = System.currentTimeMillis()
      println("endTrips: " + t.getOrElse(0))
      println("without partitionBy: " + (end2 - start))
    })


    Thread.sleep(10000)

/*
    start = System.currentTimeMillis()
    val stations2 = input2.filter(_ != header2)
                          .map(_.split(","))
                          .keyBy(_(0).toInt)
                          .partitionBy(new HashPartitioner(trips.partitions.length))
    val startTrips2 = stations2.join(trips.keyBy(_(4).toInt))
    val endTrips2 = stations2.join(trips.keyBy(_(7).toInt))

    println(startTrips2.count())
    println(endTrips2.count())
    end = System.currentTimeMillis()
    println("with partitionBy: " + (end - start))


    start = System.currentTimeMillis()
    val stations3 = input2.filter(_ != header2)
      .map(_.split(","))
      .keyBy(_(0).toInt)
      .partitionBy(new HashPartitioner(trips.partitions.length))
      .cache()
    val startTrips3 = stations3.join(trips.keyBy(_(4).toInt))
    val endTrips3 = stations3.join(trips.keyBy(_(7).toInt))

    println(startTrips3.count())
    println(endTrips3.count())
    end = System.currentTimeMillis()
    println("with partitionBy and cache: " + (end - start))


    start = System.currentTimeMillis()
    val startTrips4 = stations3.join(trips.keyBy(_(4).toInt))
    val endTrips4 = stations3.join(trips.keyBy(_(7).toInt))

    println(startTrips4.count())
    println(endTrips4.count())
    end = System.currentTimeMillis()
    println("with partitionBy and cache2: " + (end - start))
*/
    // println(startTrips.toDebugString)
    // println(endTrips.toDebugString)


    /*FlatMappedValuesRDD at join

        MappedRDD at keyBy
        MappedRDD at map
        FilteredRDD at filter
        MappedRDD
        HadoopRDD

        MappedRDD at keyBy
        MappedRDD at map
        FilteredRDD at filter
        MappedRDD
        HadoopRDD
     */

  }

}

/*
   startTrips.take(5).foreach(t => {
      val stations = t._2._1
      val trips = t._2._2
      println(t._1 + " = " +  stations.mkString(", ") + " and " + trips.mkString(", "))
    })
    endTrips.take(5).foreach(t => {
      val stations = t._2._1
      val trips = t._2._2
      println(t._1 + " = " +  stations.mkString(", ") + " and " + trips.mkString(", "))
    })
 */

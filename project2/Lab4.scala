package org.test

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.test.utils.{Station, Trip}

object Lab4 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myappname").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val path = "/home/vadim/projects/test/data/"

    val input = sc.textFile(path + "trips/*")
    val header = input.first()
    val trips = load(input, header)

    runTest(trips, "test1")
    runTest(trips.cache(), "test2")
    runTest(trips.unpersist().persist(StorageLevel.MEMORY_ONLY_SER), "test3")

    sc.stop()
    //------------------------------------------------------------------------
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Trip]))
    val sc2 = new SparkContext(conf)

    val input4 = sc2.textFile(path + "trips/*")
    val trips4 = load(input4, header)
    runTest(trips4.persist(StorageLevel.MEMORY_ONLY_SER), "test4")

    sc2.stop()
  }

  def load(input: RDD[String], header: String): RDD[Trip] = {
    input.filter(!_.equals(header))
      .map(_.split(","))
      .map(utils.Trip.parse)
  }

  def runTest(trips: RDD[Trip], name: String): Unit = {
    val number = 20
    var totalDiff = 0L
    for (i <- 1 to number) {
      val start = System.currentTimeMillis()
      val result1 = test(trips)
      result1.take(10)
      val end = System.currentTimeMillis()
      totalDiff += end - start
    }
    println("Total avg " + name + " " + (totalDiff * 1.0 / number))
  }

  def test(trips: RDD[Trip]): RDD[(Int, Double)] = {
    val byStartTerminal = trips.keyBy(_.startTerminal)
    val durationsByStart = byStartTerminal.mapValues(_.duration)
    val results = durationsByStart.aggregateByKey((0, 0.0))( (acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    val byEndTerminal = trips.keyBy(_.endTerminal)
    val durationsByEnd = byEndTerminal.mapValues(_.duration)
    val results2 = durationsByEnd.aggregateByKey((0, 0.0))( (acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    val joined = results.join(results2)

    val finalAvg = joined.mapValues(ar => {
      val pair1 = ar._1
      val pair2 = ar._2
      (pair1._1 + pair2._1) / (pair1._2 + pair2._2)
    })
    finalAvg
  }
}

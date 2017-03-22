package org.test

import org.apache.spark.{SparkConf, SparkContext}

object Lab3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myappname").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path = "/home/vadim/projects/test/data/"

    val input1 = sc.textFile(path + "trips/*")
    val header = input1.first()
    val trips = input1.filter(!_.equals(header))
                      .map(_.split(","))
                      .map(utils.Trip.parse)

    val input2 = sc.textFile(path + "stations/*")
    val header2 = input2.first()
    val stations = input2.filter(!_.equals(header2))
                         .map(_.split(","))
                         .map(utils.Station.parse)

    val byStartTerminal = trips.keyBy(_.startTerminal)
    val durationsByStart = byStartTerminal.mapValues(_.duration)
    val grouped = durationsByStart.groupByKey()
                                  .mapValues(list => list.sum / list.size)
    grouped.take(10).foreach(println)

    val results = durationsByStart.aggregateByKey((0, 0))( (acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val finalAvg = results.mapValues(i => i._1 / i._2)
    finalAvg.take(10).foreach(println)

    val firstGrouped = byStartTerminal.groupByKey().mapValues(it => it.toList.sortBy(_.startDate.getTime).head)
    println(firstGrouped.toDebugString)
    firstGrouped.take(10).foreach(println)

    val firstReduced = byStartTerminal.reduceByKey((t1, t2) => {
      t1.startDate.before(t2.startDate) match {
        case true => t1
        case false => t2
      }
    })
    println("----------")
    firstReduced.take(10).foreach(println)

    val broadcastStations = sc.broadcast(stations.keyBy(_.name).collectAsMap())
    trips.map(t => {
      (t.id, t, broadcastStations.value.getOrElse(t.startStation, None), broadcastStations.value.getOrElse(t.endStation, None))
    }).take(5).foreach(println)
  }
}

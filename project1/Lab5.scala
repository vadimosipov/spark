package labs

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Lab5 {
	val logger = Logger.getLogger(getClass.getName)

	def main(args: Array[String]) = {
		val sc = new SparkContext(new SparkConf().setAppName("lab5"))

		val trips = sc.textFile("data/trips/*")
		val stations = sc.textFile("data/stations/*")

		val result = evaluate(sc, trips, stations)

		result.saveAsTextFile("data/answer/trip_distance.txt")

		println("Press [Enter] to quit")
		Console.readLine()

		sc.stop()
	}

	def evaluate(sc: SparkContext, trips: RDD[String], stations: RDD[String]) = {
		val trips2 = trips.filter(!_.startsWith("Trip"))
			.map(_.split(","))
			.map(Trip.parse)
		trips2.collect().foreach(println)

		val stations2 = stations.filter(!_.startsWith("station_id"))
			.map(_.split(","))
			.map(Station.parse)
			.keyBy(_.name)
			.collectAsMap()
		val stations3 = sc.broadcast(stations2)
		println(stations2)

		val result = trips2.map(t => {
			val s1 = stations3.value.getOrElse(t.startStation, Station.default)
			val s2 = stations3.value.getOrElse(t.endStation, Station.default)
			println("s1: " + s1 + ", s2: " + s2)
			(t.id, distanceOf(s1.lat, s1.lon, s2.lat, s2.lon))
		})
		result
	}

	def distanceOf(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
		val earthRadius = 3963 - 13 * Math.sin(lat1)

		val dLat = Math.toRadians(lat2 - lat1)
		val dLon = Math.toRadians(lon2 - lon1)

		val a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.pow(Math.sin(dLon / 2), 2)
		val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
		val dist = earthRadius * c
		dist
	}
}

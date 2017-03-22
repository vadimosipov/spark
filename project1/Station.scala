package labs

case class Station(
                  id: Int,
                  name: String,
                  lat: Double,
                  lon: Double,
                  docks: Int,
                  landmark: String,
                  installDate: String)

object Station {
  def parse(i: Array[String]) = {
    Station(i(0).toInt, i(1), i(2).toDouble, i(3).toDouble, i(4).toInt, i(5), i(6))
  }

  val default = Station(0, "None", 0, 0, 0, "", "")
}

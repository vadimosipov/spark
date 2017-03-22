package labs

case class Trip(
                 id: Int,
                 duration: Int,
                 startDate: String,
                 startStation: String,
                 startTerminal: Int,
                 endDate: String,
                 endStation: String,
                 endTerminal: Int,
                 bike: Int,
                 subscriberType: String,
                 zipCode: Option[String]
                 )

object Trip {
  def parse(i: Array[String]) = {
    val zip = i.length match {
      case 11 => Some(i(10))
      case _ => None
    }
    Trip(i(0).toInt, i(1).toInt, i(2), i(3), i(4).toInt, i(5), i(6), i(7).toInt, i(8).toInt, i(9), zip)
  }
}

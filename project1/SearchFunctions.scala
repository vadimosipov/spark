package labs

import org.apache.spark.rdd.RDD

class SearchFunctions(val query: String) {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchesFunctionReference(rdd: RDD[String]): RDD[Boolean] = {
    rdd.map(isMatch)
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[Array[String]] = {
    rdd.map(x => x.split(query))
  }

  def getMatchesNoReference(rdd: RDD[String]): RDD[Array[String]] = {
    val query_ = query
    rdd.map(x => x.split(query_))
  }
}

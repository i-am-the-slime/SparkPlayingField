package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.menthal.spark.SparkHelper._


/**
 * Created by johnny on 29.04.15.
 */
object PositiveMinutesAggregations {

  def main(args: Array[String]) {
    val (master, dataDir) = args match {
      case Array(m, d) =>
        (m, d)
      case _ =>
        val errorMessage = "First argument is master, second directory with data"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, "PositiveMinutesAggregations")
    aggregatePosMinutes(sc, dataDir)
    sc.stop()
  }



  def aggregatePosMinutes(sc: SparkContext, dataDir: String ):Unit = {
    //read from appSessions
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //transform appSessions to posMinutes
    //write posMinutes to parquet
  }

}

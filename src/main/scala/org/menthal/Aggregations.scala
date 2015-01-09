package org.menthal

import org.apache.spark.SparkContext
import org.menthal.aggregations.AggrSpec
import org.menthal.aggregations.GeneralAggregations._
import org.menthal.model.Granularity

/**
 * Created by mark on 09.01.15.
 */
object Aggregations {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: Aggregations <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Aggregations", System.getenv("SPARK_HOME"))
    val datadir = args(1)
    //TODO actually aggregate in this method.
    aggregate(datadir, sc)
    sc.stop()
  }

  def aggregate(datadir: String,  sc: SparkContext) =
    for {
      granularity ← List(Granularity.Daily, Granularity.Weekly, Granularity.Monthly, Granularity.Yearly)
    } yield AggrSpec.aggregateGranularity(granularity, datadir, sc)

}

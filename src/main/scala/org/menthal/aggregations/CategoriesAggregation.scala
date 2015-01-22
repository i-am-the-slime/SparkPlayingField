package org.menthal.aggregations

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.menthal.aggregations.AggrSpec._
import org.menthal.aggregations.tools.{Leaf, Node, Tree}
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.Granularity
import org.menthal.model.EventType._
import org.menthal.model.Granularity.TimePeriod
import org.menthal.model.events.CCAggregationEntry
import org.menthal.model.events.Implicits._

/**
 * Created by konrad on 20.01.15.
 */
object CategoriesAggregation {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: Aggregations <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Aggregations", System.getenv("SPARK_HOME"))
    val datadir = args(1)
    aggregate(sc, datadir)
    sc.stop()
  }

  def aggregate(sc:SparkContext, datadir : String): Unit = {
    for (granularity <- granularities) {
      categorizeInParquet(sc, datadir, "appStarts", "categoryStarts", granularity)
      categorizeInParquet(sc, datadir, "appUsage", "categoryUsage", granularity)
    }
  }

  val granularities = List(
    Granularity.Hourly,
    Granularity.Daily,
    Granularity.Weekly,
    Granularity.Monthly,
    Granularity.Yearly)

  val suite: List[AggrSpec[_ <: SpecificRecord]] = List(
    AggrSpec(TYPE_APP_SESSION, toCCAppSession, agDuration("app", "usage"), agCount("app", "starts")))

  def categorize(packageName: String): String = {
    categoriesLookup(packageName)
  }

 val categoriesLookup: Map[String, String] = Map()


  def readLookupFromFile() = {

  }


  def transformAggregationsInParquet(fn:  RDD[CCAggregationEntry] => RDD[CCAggregationEntry])
                                    (sc: SparkContext, datadir: String, inputAggrName: String, outputAggrName: String, granularity:TimePeriod): Unit = {
    val inputRDD = ParquetIO.readAggrType(sc, datadir, inputAggrName, granularity).map(toCCAggregationEntry)
    val outputRDD = fn(inputRDD)
    ParquetIO.writeAggrType(sc, datadir, outputAggrName, granularity, outputRDD.map(_.toAvro))
  }

  def categorizeAggregations(aggregation: RDD[CCAggregationEntry]):RDD[CCAggregationEntry] = {
    val categories = for (
      CCAggregationEntry(userId, time, granularity, packageName, value) <- aggregation
    ) yield ((userId, time, granularity, categorize(packageName)), value)

    categories.foldByKey(0)(_ + _).map { case ((userId, time, granularity, category), value) =>
      CCAggregationEntry(userId, time, granularity, category, value)
    }
  }

  def categorizeInParquet = transformAggregationsInParquet(categorizeAggregations) _
}



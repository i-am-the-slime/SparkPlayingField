package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.{AggregationType, Granularity}
import org.menthal.model.Granularity.TimePeriod
import org.menthal.model.events.CCAggregationEntry
import org.menthal.model.events.Implicits._
import org.menthal.spark.SparkHelper.getSparkContext

import scala.util.Try

/**
 * Created by konrad on 20.01.15.
 */
object CategoriesAggregation {
  def name = "CategoriesAggregation"

  def main(args: Array[String]) {
    val (master, datadir, lookupFile) = args match {
      case Array(m, d, l) => (m,d, l)
      case Array(m, d) => (m,d, d + "/categories.csv")
      case _ =>
        val errorMessage = "First argument is master, second datdir path, optional third argument is categories lookup path"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, name)
    aggregateCategories(sc, datadir, lookupFile, List(Granularity.Hourly, Granularity.Daily, Granularity.Weekly, Granularity.Monthly, Granularity.Yearly))
    sc.stop()
  }

  def csvLineToMapTuple(line:String):Option[(String, String)] = Try{
    val split = line.split(",")
    (split(1), split(2))
  }.toOption

  def readLookupFromFile(sc:SparkContext, path:String):collection.Map[String, String] = {
    val file = sc.textFile(path)
    val mapTuples = for {
      line ← file
      (packageName, category) ← csvLineToMapTuple(line)
    } yield (packageName, category)
    mapTuples.collectAsMap()
  }

  def transformAggregationsInParquet(sc: SparkContext,
                                     categorizeFunction: RDD[CCAggregationEntry] => RDD[CCAggregationEntry],
                                     datadir:String)(inputAggrName: String, outputAggrName: String, granularity:TimePeriod)
                                    : Unit = {
    val inputRDD = ParquetIO.readAggrType(sc, datadir, inputAggrName, granularity).map(toCCAggregationEntry)
    val outputRDD = categorizeFunction(inputRDD)
    ParquetIO.writeAggrType(sc, datadir, outputAggrName, granularity, outputRDD.map(_.toAvro))
  }

  def aggregateCategories(sc:SparkContext,
                          datadir : String,
                          lookupFile:String,
                          granularities:List[TimePeriod] = Granularity.all): Unit = {
    val categoriesLookup:Broadcast[collection.Map[String, String]] = sc.broadcast(readLookupFromFile(sc, lookupFile))
    def categorize(packageName: String): String = {
      categoriesLookup.value.getOrElse(packageName, "unknown")
    }
    def categorizeAggregations(aggregation: RDD[CCAggregationEntry]):RDD[CCAggregationEntry] = {
      val categories = for (
        CCAggregationEntry(userId, time, granularity, packageName, value) <- aggregation
      ) yield ((userId, time, granularity, categorize(packageName)), value)
      categories.foldByKey(0)(_ + _) map { case ((userId, time, granularity, category), value) =>
        CCAggregationEntry(userId, time, granularity, category, value)
      }
    }
    def transformAggregations = transformAggregationsInParquet(sc, categorizeAggregations, datadir) _

    for (granularity <- granularities) {
      transformAggregations(AggregationType.AppTotalCount, "category_total_count", granularity)
      transformAggregations(AggregationType.AppTotalDuration, "category_total_duration", granularity)
    }
  }
}



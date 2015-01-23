package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.menthal.aggregations.tools.AggrSpec
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.Granularity
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
  var categoriesLookup:collection.Map[String, String] = collection.Map()

  def main(args: Array[String]) {
    val (master, datadir, lookupFile) = args match {
      case Array(m, d, l) => (m,d, l)
      case Array(m, d) => (m,d, d + "/categories.csv")
      case _ =>
        val errorMessage = "First argument is master, second datdir path, optional third argument is categories lookup path"
        throw new IllegalArgumentException(errorMessage)
    }
    implicit val sc = getSparkContext(master, name)
    categoriesLookup = readLookupFromFile(lookupFile)
    aggregate(sc, datadir)
    sc.stop()
  }

  def aggregate(sc:SparkContext, datadir : String): Unit = {
    for (granularity <- granularities) {
      categorizeInParquet(sc, datadir, "app_starts", "category_starts", granularity)
      categorizeInParquet(sc, datadir, "app_usage", "category_usage", granularity)
    }
  }

  val granularities = List(
    Granularity.Hourly,
    Granularity.Daily,
    Granularity.Weekly,
    Granularity.Monthly,
    Granularity.Yearly)


  def categorize(packageName: String): String = {
    categoriesLookup.getOrElse(packageName, "unknown")
  }

  def readLookupFromFile(path:String)(implicit sc:SparkContext):collection.Map[String, String] = {
    val file = sc.textFile(path)
    (for {
      line ← file
      (packageName, category) ← csvLineToMapTuple(line)
    } yield (packageName, category)).collectAsMap()
  }

  def csvLineToMapTuple(line:String):Option[(String, String)] = Try{
    val split = line.split(",")
    (split(1), split(2))
  }.toOption

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



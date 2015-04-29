package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.menthal.aggregations.tools.{SleepFinder, WindowFunctions}
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.{AggregationType, Granularity}
import org.menthal.model.Granularity.{TimePeriod, durationInMillis}
import org.menthal.model.events.CCAggregationEntry
import org.menthal.model.events.Implicits._
import org.menthal.spark.SparkHelper._



/**
 * Created by konrad on 30/03/15.
 */
object SleepAggregations {

  def main(args: Array[String]) {
    val (master, dataDir) = args match {
      case Array(m, d) =>
        (m, d)
      case _ =>
        val errorMessage = "First argument is master, second directory with data"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, "SleepAggregation")
    aggregateSleep(sc, dataDir)
    sc.stop()
  }

  implicit val granularity: TimePeriod = Granularity.FiveMin

  case class CCSignalWindow(userId: Long, timeWindow:Long, value: Long)

  case class CCMedianDailyProfile(userId: Long, timeWindow: Long, medianUsage: Long)

  case class CCNoInteractionPeriod(userId:Long, startTime:Long, endTime:Long, duration:Long)

  type SignalWindow = ((Long, Long), Long)

  def toDiscreetSignal(windowUsage: RDD[CCAggregationEntry]): RDD[SignalWindow] = {
    val windows = for {
      CCAggregationEntry(userId, time, _, _, usage) <- windowUsage
    } yield ((userId, time), usage)
    windows.cache()
    val mins = windows.keys.reduceByKey(_ min _)
    val maxes = windows.keys.reduceByKey(_ max _)
    val limits = mins.join(maxes)
    val emptyWindows = for {
      (userId, (min, max)) <- limits
      time <- min to max by durationInMillis(granularity)
    } yield ((userId, time), 0L)
    val fullSignal = (windows ++ emptyWindows).reduceByKey(_ + _)
    fullSignal
  }

  def calculateDailyMedianProfile(usageWindows: RDD[SignalWindow]): RDD[CCMedianDailyProfile] = {
    val dailyUsageWindows = for {
      ((userId, time), usage) <- usageWindows
    } yield ((userId, time % durationInMillis(Granularity.Daily)), usage)
    val medianDailyProfile = dailyUsageWindows.groupByKey.mapValues(WindowFunctions.median)
    for {
      ((userId, timeWindow), medianUsage) <- medianDailyProfile
    } yield CCMedianDailyProfile(userId, timeWindow, medianUsage)
  }


  def calculateMedianFilter(usageWindows: RDD[SignalWindow]):RDD[CCSignalWindow] = {
    val dailyUsageWindows = for {
      ((userId, time), usage) <- usageWindows
    } yield (userId, (time , usage))
    val medianFilterProfile = usageWindows.groupByKey.flatMapValues(WindowFunctions.medianFilter)
    for {
      ((userId, timeWindow), medianUsage) <- medianFilterProfile
    } yield CCSignalWindow(userId, timeWindow, medianUsage)
  }

  def calculateMedianSleep(dailyProfile: RDD[CCMedianDailyProfile]):RDD[CCNoInteractionPeriod] = {
    //We concatened median daily profile with itself with time moved by a day
    //This way we capture sleep happening at change of day
    val repeatedDailyProfile = for {
      CCMedianDailyProfile(userId, timeWindow, usage) <- dailyProfile
      time <- List(timeWindow,  timeWindow + Granularity.millisPerDay)
    } yield CCSignalWindow(userId, time, usage)
    calculateSleep(repeatedDailyProfile)
  }

  def calculateSleep(signalWindows: RDD[CCSignalWindow]):RDD[CCNoInteractionPeriod] = {
    signalWindows.groupBy(_.userId).values.map(_.toList.sortBy(_.timeWindow)).flatMap(SleepFinder.findLongestSleepTimes(granularity))
  }

  def aggregateSleep(sc : SparkContext, dataPath: String):Unit = {
    val aggregations = ParquetIO.readAggrType(sc, dataPath, AggregationType.AppTotalDuration, granularity).map(toCCAggregationEntry)
    val signal = toDiscreetSignal(aggregations)
    signal.cache()
    val medianDailyProfile = calculateDailyMedianProfile(signal)
    val filteredSignal = calculateMedianFilter(signal)
    val medianSleep = calculateMedianSleep(medianDailyProfile)
    val dailySleep = calculateSleep(filteredSignal)
  }
}

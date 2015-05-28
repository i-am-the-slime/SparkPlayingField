package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.rdd.RDD

import org.menthal.aggregations.tools.{SleepFinder, WindowFunctions}
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.{AggregationType, Granularity}
import org.menthal.model.Granularity.{TimePeriod, durationInMillis, roundTimestamp}
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

  type User = Long
  type Usage = Long
  type Time = Long
  type UserWindow = (User, Usage)
  type Signal = (Time, Usage)
  type SignalWindow = (User, Time, Usage)
  type SignalWindowPairByUserWindow = (UserWindow, Usage)
  type SignalWindowPairByUser = (User, Signal)


  def toDiscreetSignal(windowUsage: RDD[CCAggregationEntry]): RDD[CCSignalWindow] = {
    val windows:RDD[SignalWindowPairByUserWindow] = for {
      CCAggregationEntry(userId, time, _, _, usage) <- windowUsage
    } yield ((userId, time), usage)
    windows.cache()

    val mins = windows.keys.reduceByKey(_ min _)
    val maxes = windows.keys.reduceByKey(_ max _)
    val limits = mins.join(maxes)

    val emptyWindows:RDD[SignalWindowPairByUserWindow] = for {
      (userId, (min, max)) <- limits
      time <- min to max by durationInMillis(granularity)
    } yield ((userId, time), 0L)

    val fullSignalRDD:RDD[SignalWindowPairByUserWindow] = (windows ++ emptyWindows).reduceByKey(_ + _)

    for {
      ((userId, time), usage) <- fullSignalRDD
    } yield CCSignalWindow(userId, time, usage)
  }

  def calculateDailyMedianProfile(usageWindows: RDD[CCSignalWindow]): RDD[CCMedianDailyProfile] = {
    val dailyUsageWindows:RDD[SignalWindowPairByUserWindow] = for {
      CCSignalWindow(userId, time, usage) <- usageWindows
    } yield ((userId, time % durationInMillis(Granularity.Daily)), usage)

    val medianDailyProfile:RDD[SignalWindowPairByUserWindow] = dailyUsageWindows.groupByKey.mapValues(WindowFunctions.median)
    for {
      ((userId, timeWindow), medianUsage) <- medianDailyProfile
    } yield CCMedianDailyProfile(userId, timeWindow, medianUsage)
  }

  def getDaysOfNoUsage(usageWindows: RDD[CCSignalWindow]):RDD[UserWindow] = {
    val usageByUserDailyWindow:RDD[SignalWindowPairByUserWindow] = for {
      CCSignalWindow(userId, time, usage) <- usageWindows
    } yield ((userId, roundTimestamp(time, Granularity.Daily)), usage)

    val usageByUserDailyWindowReduced = usageByUserDailyWindow.reduceByKey(_ + _)
    usageByUserDailyWindowReduced filter { case ((user, day), usage) => usage == 0 } keys
  }


  def filterOutDaysOfNoUsage(sc: SparkContext, usageWindows: RDD[CCSignalWindow]):RDD[CCSignalWindow] = {
    val userWindowsWithNoUsage:RDD[UserWindow] = getDaysOfNoUsage(usageWindows)
    val userWindowsWithNoUsageBroadcast = sc.broadcast(userWindowsWithNoUsage.collect().toSet)

    val  usageByUserDayPair:RDD[(UserWindow, Time, Usage)] = for {
      CCSignalWindow(userId, time, usage) <- usageWindows
    } yield ((userId, roundTimestamp(time, Granularity.Daily)), time, usage)

    val filtered =
      usageByUserDayPair filter { case (uw:UserWindow, time: Long, usage:Long) =>
                                !userWindowsWithNoUsageBroadcast.value.contains(uw)}

    for {
      ((userId, timeWindow), time, usage) <- filtered
    } yield CCSignalWindow(userId, time, usage)
  }

//  def calculateDailyMedianProfileFromDF(usageWindows: DataFrame): RDD[CCMedianDailyProfile] = {
//    val dailyUsageWindows = for {
//       row <- usageWindows
//       userId = row.getLong(0)
//       time = row.getLong(1)
//       usage = row.getLong(2)
//    } yield ((userId, time % durationInMillis(Granularity.Daily)), usage)
//    val medianDailyProfile = dailyUsageWindows.groupByKey.mapValues(WindowFunctions.median)
//    for {
//      ((userId, timeWindow), medianUsage) <- medianDailyProfile
//    } yield CCMedianDailyProfile(userId, timeWindow, medianUsage)
//  }


  def calculateMedianFilter(usageWindows: RDD[CCSignalWindow]):RDD[CCSignalWindow] = {
    val dailyUsageWindows = for {
      CCSignalWindow(userId, time, usage) <- usageWindows
    } yield (userId, (time , usage))
    val windowsGroupedByUsers = dailyUsageWindows.groupByKey

    val sortedWindowsGroupedByUsers:RDD[(User,List[(Time, Usage)])] =
      windowsGroupedByUsers.mapValues(_.toList sortBy {case (time,usage) => time})

    val medianDailyProfilesGroupedByUser:RDD[(User,List[(Time, Usage)])] =
      sortedWindowsGroupedByUsers.mapValues(WindowFunctions.medianFilterWithIndex)

    for {
      (userId, timeIndexedFilteredUsage) <- medianDailyProfilesGroupedByUser
      (time, medianUsage) <- timeIndexedFilteredUsage
    } yield CCSignalWindow(userId, time, medianUsage)
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
    signalWindows.groupBy(_.userId).values
                 .map(_.toList.sortBy(_.timeWindow))
                 .flatMap(SleepFinder.findLongestSleepTimes(granularity))
  }



  def aggregateSleep(sc : SparkContext, dataPath: String):Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //to signal
    val aggregations = ParquetIO.readAggrType(sc, dataPath, AggregationType.AppTotalDuration, granularity).map(toCCAggregationEntry)
    val signalRDD:RDD[CCSignalWindow] = toDiscreetSignal(aggregations)

    //NO FILTERS
    val cleanNoFilterSignal = filterOutDaysOfNoUsage(sc, signalRDD)
    cleanNoFilterSignal.cache()
    cleanNoFilterSignal.toDF().saveAsParquetFile(dataPath + "/noFilterSignal")

    val noFilterDailySleep = calculateSleep(cleanNoFilterSignal)
    noFilterDailySleep.toDF().saveAsParquetFile(dataPath + "/noFilterDailySleep")

    val noFilterMedianDailyProfile = calculateDailyMedianProfile(cleanNoFilterSignal)
    noFilterMedianDailyProfile.cache()
    cleanNoFilterSignal.unpersist()
    noFilterMedianDailyProfile.toDF().saveAsParquetFile(dataPath + "/noFilterMedianDailyProfile")

    val noFilterMedianSleep = calculateMedianSleep(noFilterMedianDailyProfile)
    noFilterMedianSleep.toDF().saveAsParquetFile(dataPath + "/noFilterMedianDailySleep")
    noFilterMedianDailyProfile.unpersist()

    //MEDIAN FILTER
    val medianFilteredSignal = calculateMedianFilter(signalRDD)
    //Saving and caching
    //medianFilteredSignal.cache()
    //medianFilteredSignal.toDF().saveAsParquetFile(dataPath + "/medianFilteredSignal")

    val cleanSignal = filterOutDaysOfNoUsage(sc, medianFilteredSignal)
    cleanSignal.cache()
    cleanSignal.toDF().saveAsParquetFile(dataPath + "/cleanMedianFilteredSignal")

    val dailySleep = calculateSleep(cleanSignal)
    dailySleep.toDF().saveAsParquetFile(dataPath + "/dailySleep")

    val medianDailyProfile = calculateDailyMedianProfile(cleanSignal)
    medianDailyProfile.cache()
    cleanSignal.unpersist()
    medianDailyProfile.toDF().saveAsParquetFile(dataPath + "/medianDailyProfile")

    val medianSleep = calculateMedianSleep(medianDailyProfile)
    medianSleep.toDF().saveAsParquetFile(dataPath + "/medianDailySleep")
    medianDailyProfile.unpersist()

  }
}

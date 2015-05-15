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

  type UserWindow = (Long,Long)
  type SignalWindow = (UserWindow, Long)

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

  def calculateDailyMedianProfile(usageWindows: RDD[CCSignalWindow]): RDD[CCMedianDailyProfile] = {
    val dailyUsageWindows = for {
      CCSignalWindow(userId, time, usage) <- usageWindows
    } yield ((userId, time % durationInMillis(Granularity.Daily)), usage)
    val medianDailyProfile = dailyUsageWindows.groupByKey.mapValues(WindowFunctions.median)
    for {
      ((userId, timeWindow), medianUsage) <- medianDailyProfile
    } yield CCMedianDailyProfile(userId, timeWindow, medianUsage)
  }

  def getDaysOfNoUsage(usageWindows: DataFrame):RDD[UserWindow] = {
    val usageByUserDayPair = for {
      row <- usageWindows
      userId = row.getLong(0)
      time = row.getLong(1)
      usage = row.getLong(2)
    } yield ((userId, roundTimestamp(time, Granularity.Daily)), usage)
    val totalUsageByUserDayPairRDD = usageByUserDayPair.reduceByKey(_ + _)
    totalUsageByUserDayPairRDD filter { case ((user, day), usage) => usage == 0 } keys
  }


  def filterOutDaysOfNoUsage(sc: SparkContext, usageWindows: DataFrame):RDD[CCSignalWindow] = {
    val userDayPairWithNoUsageRDD = getDaysOfNoUsage(usageWindows)
    val userWindowsWithNoUsageBroadcast = sc.broadcast(userDayPairWithNoUsageRDD.collect().toSet)
    val usageByUserDayPair = for {
      row <- usageWindows
      userId = row.getLong(0)
      time = row.getLong(1)
      usage = row.getLong(2)
    } yield ((userId, roundTimestamp(time, Granularity.Daily)), time, usage)
    val filtered:RDD[(UserWindow,Long,Long)] = usageByUserDayPair filter { case (uw:UserWindow, time: Long, usage:Long) =>
                                !userWindowsWithNoUsageBroadcast.value.contains(uw)}
    for {
      ((userId, timeWindow), time,usage) <- filtered
    } yield CCSignalWindow(userId, time, usage)
  }

  def calculateDailyMedianProfileFromDF(usageWindows: DataFrame): RDD[CCMedianDailyProfile] = {
    val dailyUsageWindows = for {
       row <- usageWindows
       userId = row.getLong(0)
       time = row.getLong(1)
       usage = row.getLong(2)
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
    val windowsGroupedBeUsers = dailyUsageWindows.groupByKey
    val sortedWindowsGroupedByUsers:RDD[(Long,List[(Long,Long)])] =
      windowsGroupedBeUsers.mapValues(_.toList sortBy {case (time,usage) => time})
    val medianDailyProfilesGroupedByUser:RDD[(Long,List[(Long,Long)])] =
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


    //val aggregations = ParquetIO.readAggrType(sc, dataPath, AggregationType.AppTotalDuration, granularity).map(toCCAggregationEntry)
    //val signal = toDiscreetSignal(aggregations)


    ////val medianDailyProfile = calculateDailyMedianProfile(signal)
    ////medianDailyProfile.cache()
    ////medianDailyProfile.toDF().saveAsParquetFile(dataPath + "/medianDailyProfile")

    //val filteredSignal = calculateMedianFilter(signal)
    //Saving and caching
    //filteredSignal.cache()
    //filteredSignal.toDF().saveAsParquetFile(dataPath + "/medianFilteredSignal")

    //val dailySleep = calculateSleep(filteredSignal)
    ////Saving and caching
    //dailySleep.toDF().saveAsParquetFile(dataPath + "/dailySleep")

    val medianFilteredSignal = sqlContext.parquetFile(dataPath + "/medianFilteredSignal")
    val cleaneadSignal = filterOutDaysOfNoUsage(sc, medianFilteredSignal)
    val medianDailyProfile = calculateDailyMedianProfile(cleaneadSignal)
    //val medianDailyProfile = calculateDailyMedianProfile(filteredSignal)
    ////Saving and caching
    medianDailyProfile.cache()
    medianDailyProfile.toDF().saveAsParquetFile(dataPath + "/medianDailyProfile")

    val medianSleep = calculateMedianSleep(medianDailyProfile)
    ////Saving and caching
    medianSleep.toDF().saveAsParquetFile(dataPath + "/medianDailySleep")
    medianDailyProfile.unpersist()



    val aggregations = ParquetIO.readAggrType(sc, dataPath, AggregationType.AppTotalDuration, granularity).map(toCCAggregationEntry)
    val discreetSignal = for {
      ((userId, time), usage) <- toDiscreetSignal(aggregations)
    } yield CCSignalWindow(userId, time, usage)
    val cleanedNoFilteredSignal = filterOutDaysOfNoUsage(sc, discreetSignal.toDF())
    cleanedNoFilteredSignal.cache()
    cleanedNoFilteredSignal.toDF().saveAsParquetFile(dataPath + "/nonfilteredSignal")
    val simpleMedianDailyProfile = calculateDailyMedianProfile(cleanedNoFilteredSignal)
    simpleMedianDailyProfile.cache()
    cleanedNoFilteredSignal.unpersist()
    simpleMedianDailyProfile.toDF().saveAsParquetFile(dataPath + "/simpleMedianDailyProfile")
    val simpleMedianSleep = calculateMedianSleep(simpleMedianDailyProfile)
    ////Saving and caching
    simpleMedianSleep.toDF().saveAsParquetFile(dataPath + "/simpleMedianDailySleep")
    simpleMedianDailyProfile.unpersist()

  }
}

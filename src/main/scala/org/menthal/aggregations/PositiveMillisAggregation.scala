package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.joda.time.DateTime
import org.menthal.aggregations.tools.EventTransformers.getSplittingTime
import org.menthal.model.Granularity
import org.menthal.model.Granularity._
import org.menthal.spark.SparkHelper._

/**
 * Created by konrad on 12/06/15.
 */
object PositiveMillisAggregation {
  def main(args: Array[String]) {
    val (master, dataDir) = args match {
      case Array(m, d) =>
        (m, d)
      case _ =>
        val errorMessage = "First argument is master, second directory with data"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, "PositiveMinutesAggregations")
    aggregatePositiveMillis(sc, dataDir)
    sc.stop()
  }

  val filterStart:Timestamp = new DateTime(2013, 1, 1, 0, 0).getMillis
  case class CCPositiveMillis(val userId:Long, val day:Long, val granularity:Int, val interval: Int, val minutes: Long)

  val supportedGranularities = List(Granularity.Daily, Granularity.Hourly)
  val minIntervals = List(0, 5, 10, 15, 20, 30, 60)
  val millisInMin = 60 * 1000
  val positiveMillisPath = "/pos_millis"

  type UserId = Long
  type Interval = Int
  type Time = Long
  type DurationsByUserTimeIntervals = ((UserId, Time), Long)
  type PositiveMillisByUserTimeInterval = ((UserId, Interval, Time), Long)
  def getPositiveMillis(duration: Long, interval :Int): Long =
    Math.max(0, duration - (interval * millisInMin))


  def inactivitySessionToDurations(row: Row, granularity: TimePeriod):List[DurationsByUserTimeIntervals] = {
    val userId = row.getLong(0)
    val time = row.getLong(1)
    val duration = row.getLong(2)
    if ((time > filterStart) && (duration < millisPerDay))
      for ((start, duration) ← getSplittingTime(time, duration, granularity))
        yield ((userId, roundTimestamp(start, granularity)), duration)
  }

  def toCCPositiveMillis(granularity: TimePeriod)(positiveMillisByUserTimeInterval: PositiveMillisByUserTimeInterval):CCPositiveMillis = {
    val ((userId, interval, time), millis) = positiveMillisByUserTimeInterval
    return CCPositiveMillis(userId, time, granularity, interval, millis)
  }

 def getPositiveMillis(inactivitySessionsDf: DataFrame, granularity: TimePeriod):RDD[CCPositiveMillis] = {
   val positiveMillisByUserTimeIntervals: RDD[PositiveMillisByUserTimeInterval] = for {
     row <- inactivitySessionsDf
     ((userId, timeWindow), duration) <- inactivitySessionToDurations(row, granularity)
     interval <- minIntervals
   } yield ((userId, interval, timeWindow), getPositiveMillis(duration, interval))
   positiveMillisByUserTimeIntervals.reduceByKey(_ + _).map(toCCPositiveMillis(granularity) _)
 }

  def aggregatePositiveMillis(sc: SparkContext, dataDir: String):Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //read from appSessions
    val inactivitySessionsDf = sqlContext.parquetFile(dataDir + PhoneSessionsAggregation.phoneInactiveSessionsPath).cache()
    for (granularity <- supportedGranularities) {
      val positiveMillis = getPositiveMillis(inactivitySessionsDf, granularity)
      positiveMillis.toDF().saveAsParquetFile(dataDir + positiveMillisPath + '/' + Granularity.asString(granularity))
    }
  }

}

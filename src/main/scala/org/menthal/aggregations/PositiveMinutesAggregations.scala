package org.menthal.aggregations

import org.apache.spark.SparkContext
import scala.collection.mutable.Map

import org.apache.spark.rdd.RDD
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.{Granularity, EventType}
import org.menthal.model.events.{CCAppSession, AppSession}
import org.menthal.spark.SparkHelper._
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.menthal.model.events.Implicits._
import org.apache.spark.SparkContext._

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

  case class CCActivityChange(val userId:Long, val time:Long, val actionType:Short)
  case class CCPositiveMinutes(val userId:Long, val day:Long, val interval: Int, val minutes: Long)

  def endOfDayTime(time:Long):Long = Granularity.roundTimestampCeiling(time, Granularity.Daily) -1
  def startOfDayTime(time:Long):Long = Granularity.roundTimestamp(time, Granularity.Daily)

  def splitAppSessionsFunction(appSession:CCAppSession):List[CCActivityChange] = {
    val currentEndingEventTime = appSession.time + appSession.duration
    val evStart = CCActivityChange(appSession.userId, appSession.time, 1)
    val evEnd = CCActivityChange(appSession.userId, currentEndingEventTime, 0)
    if (currentEndingEventTime > endOfDayTime(appSession.time)) {
      val evTillMidnight = evEnd.copy(time = endOfDayTime(appSession.time))
      val evFromMidnight = evStart.copy(time = startOfDayTime(currentEndingEventTime))
      List(evStart, evTillMidnight, evFromMidnight, evEnd)
    } else {
      List(evStart,evEnd)
    }
  }

  def splitAppSessionsByDays(appSessions: RDD[CCAppSession]):RDD[CCActivityChange] ={
    appSessions.flatMap(splitAppSessionsFunction)
    //The same as
    //for { session <- appSessions
    //    splittedSession <- splitAppSessionsFunction(session)
    //} yield splittedSession
  }

//  def getPositiveMinuteFromActivityChanges(activityTimes:Iterable[Long]):Long = {
//
//  }

  val START_TYPE = 1
  val STOP_TYPE = 0

  val millisInMin = 60 * 1000
  val minIntervals = List(0, 5, 10, 15, 20, 30, 60)

  def getPositiveTimeFromActivityChanges(activityTimes:Iterable[CCActivityChange]):Map[Int,Long] = {
    val sortedArrayActivityTimes:Array[CCActivityChange] = activityTimes.toArray.sortBy(_.time)
    //var posMinutes:Long = 0L
    val posTime: Map[Int, Long] = Map()

    var i:Int = 0

    //Skipping entries until first action type is stop
    do {
      i=i+1
    } while (sortedArrayActivityTimes(i).actionType == START_TYPE)
    var currentEventTime:Long = sortedArrayActivityTimes(i).time

    for (j <- i until activityTimes.size) {
      //write an explanation if this works
      if (sortedArrayActivityTimes(j).actionType == START_TYPE &&
        sortedArrayActivityTimes(j - 1).actionType == STOP_TYPE)
      {
        val breakTime:Long = sortedArrayActivityTimes(j).time - currentEventTime //how long time off phone was
        for (interval <- minIntervals) {
          val timeAfterBreak:Long = math.max(breakTime - interval * millisInMin, 0L)
          val newPosTime = timeAfterBreak + posTime.getOrElse(interval, 0L)
          posTime(interval) = newPosTime
        }
      }
      currentEventTime = sortedArrayActivityTimes(j).time
    }
    posTime
  }

  type User = Long
  type Day = Long


  def transformAppSessionsToPosMinutes (appSessions: RDD[CCAppSession]):RDD[CCPositiveMinutes] = {
    val activityChanges = splitAppSessionsByDays(appSessions)
    //types are not necessary - only for help
    val userChangesGrouped:RDD[((User, Day), Iterable[CCActivityChange])] =
      activityChanges.groupBy(activityChange => (activityChange.userId, startOfDayTime(activityChange.time)))
//    val userChangesGrouped = activityChanges.map(a => ((a.userId, begginingDayTime(a.time)), a.time)).groupByKey

    val userPosTimeByDay:RDD[((User,Day), Map[Int,Long])] = userChangesGrouped.mapValues(getPositiveTimeFromActivityChanges)
    for {
      ((userId, day), posTime) <- userPosTimeByDay
      (interval, posTimeForInterval) <- posTime
    } yield CCPositiveMinutes(userId,day, interval, posTimeForInterval)
  }

  def aggregatePosMinutes(sc: SparkContext, dataDir: String ):Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //read from appSessions
    val app_sessions:RDD[CCAppSession] = ParquetIO.readEventType(sc, dataDir, EventType.TYPE_APP_SESSION).map(toCCAppSession)
    //transform appSessions to posMinutes
    val posMinutes:RDD[CCPositiveMinutes] = transformAppSessionsToPosMinutes(app_sessions)
    //write posMinutes to parquet
    posMinutes.toDF().saveAsParquetFile(dataDir+"/pos_minutes")
  }

}

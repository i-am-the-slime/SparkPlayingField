package org.menthal.aggregations

import org.apache.spark.SparkContext


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
  case class CCPositiveMinutes(val userId:Long, val day:Long, val minutes: Long)

  def endingDayTime(time:Long):Long = Granularity.roundTimestampCeiling(time, Granularity.Daily) -1
  def begginingDayTime(time:Long):Long = Granularity.roundTimestamp(time, Granularity.Daily)

  def splitAppSessionsFunction(appSession:CCAppSession):List[CCActivityChange] = {
    val currentEndingEventTime = appSession.time + appSession.duration
    val evStart = CCActivityChange(appSession.userId, appSession.time, 1)
    val evEnd = CCActivityChange(appSession.userId, currentEndingEventTime, 0)
    if (currentEndingEventTime > endingDayTime(appSession.time)) {
      val evTillMidnight = evEnd.copy(time = endingDayTime(appSession.time))
      val evFromMidnight = evStart.copy(time = begginingDayTime(currentEndingEventTime))
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

  def getPositiveMinuteFromActivityChanges(activityTimes:Iterable[CCActivityChange]):Long = {
    val sortedArrayActivityTimes:Array[CCActivityChange] = activityTimes.toArray.sortBy(_.time)
    var posMinutes:Long = 0L
    var i:Int = 0
    while (sortedArrayActivityTimes(i).actionType != 1){
      i=i+1
    }
    var currentEventTime:Long = sortedArrayActivityTimes(i).time
    for (j <- i until activityTimes.size) {
      if (sortedArrayActivityTimes(j).actionType == 1){
        currentEventTime = sortedArrayActivityTimes(j).time
      } else {
        posMinutes += sortedArrayActivityTimes(j).time - currentEventTime
        currentEventTime = sortedArrayActivityTimes(j).time
      }
    }
    posMinutes
  }

  def transformAppSessionsToPosMinutes (appSessions: RDD[CCAppSession]):RDD[CCPositiveMinutes] = {
    val activityChanges = splitAppSessionsByDays(appSessions)
    //types are not necessary - only for help
    val userChangesGrouped:RDD[((Long,Long), Iterable[CCActivityChange])] =
      activityChanges.groupBy(activityChange => (activityChange.userId, begginingDayTime(activityChange.time)))
//    val userChangesGrouped = activityChanges.map(a => ((a.userId, begginingDayTime(a.time)), a.time)).groupByKey

    val userMinutesByDay:RDD[((Long,Long), Long)] = userChangesGrouped.mapValues(getPositiveMinuteFromActivityChanges)
    for {
      ((userId, day),minutes) <- userMinutesByDay
    } yield CCPositiveMinutes(userId,day,minutes)
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

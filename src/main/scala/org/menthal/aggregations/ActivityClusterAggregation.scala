package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.menthal.aggregations.PhoneSessionsAggregation.CCPhoneSessionFragment
import org.menthal.model.Granularity
import org.menthal.model.events._
import org.menthal.spark.SparkHelper._
import org.menthal.model.events.{MenthalEvent, CCAppSession}
import org.menthal.aggregations.SleepAggregations.CCNoInteractionPeriod

/**
 * Created by konrad on 30/10/15.
 */
object ActivityClusterAggregation {


  def main(args: Array[String]) {
    val (master, datadir, breaksNumber) = args match {
      case Array(m, d, n) => (m, d, n.toInt)
      case Array(m, d) =>  (m, d, defaultBreaksNumber)
      case _ =>
        val errorMessage = "First argument is master, second input/output path"
        throw new IllegalArgumentException(errorMessage)
    }

    val sc = getSparkContext(master, name)
    parquetToActivityClusterFragment(sc, datadir, breaksNumber)
    sc.stop()
  }

  val inactivitySessionName = "BREAK"
  type InactivitySessionRank = Int
  type InactivitySessionEnd = Long
  type InactivitySessionState = (InactivitySessionRank, InactivitySessionEnd)
  type InactivitySessionAccumulator = (InactivitySessionState, List[CCActivityClusterFragment])

  case class CCActivityClusterFragment(userId: Long,  time: Long,
                                       duration: Long, app: String,
                                       clusterStartTime: Long,
                                       lastInactivitySessionRank: Int)


  def parquetToActivityClusterFragment(sc: SparkContext, datadir: String, breaksNumber: Int): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val phoneSessions = sqlContext.read.parquet(datadir + "/phone_sessions")
    val sessions:RDD[CCActivityClusterFragment] = for {
      row <- phoneSessions
      userId = row.getLong(0)
      time = row.getLong(2)
      duration = row.getLong(3)
      name = row.getString(4)
    } yield CCActivityClusterFragment(userId, time, duration, name, 0, -1)
    val inactivitySessionsDf = sqlContext.read.parquet(datadir + PhoneSessionsAggregation.phoneInactiveSessionsPath)
    val noInteractionPeriods = for {
      row <- inactivitySessionsDf
      userId = row.getLong(0)
      startTime = row.getLong(1)
      duration = row.getLong(2)
      endTime = startTime + duration
    } yield CCNoInteractionPeriod(userId, startTime, endTime, duration)
    val breaks = findBreak(breaksNumber, noInteractionPeriods)
    val sessionsAndBreaks: RDD[CCActivityClusterFragment] = sessions ++ breaks
    val activityClusterFragments = toActivityClusterFragments(sessionsAndBreaks)
    activityClusterFragments.toDF().saveAsParquetFile(datadir + "/activity_clusters_" + breaksNumber.toString)
  }



  val name: String = "ActivityClusterAggregation"
  val defaultBreaksNumber = 2

  type PhoneInactivitySessions = CCPhoneSessionFragment
  def findBreak(n: Int, noInteractionPeriods: RDD[CCNoInteractionPeriod]):RDD[CCActivityClusterFragment] = {
    def topNBreaksWithRank(breaks:Iterable[CCNoInteractionPeriod]):List[(CCNoInteractionPeriod, Int)] =
      breaks.toList.sortBy(-_.duration).take(n).zipWithIndex
    val longestInactivityByDaysWithRank = noInteractionPeriods
                        .groupBy(period => (period.userId, Granularity.roundTimestamp(period.endTime, Granularity.Daily)))
                        .values
                        .flatMap(topNBreaksWithRank)
    for {
      (inactivity, rank) <- longestInactivityByDaysWithRank
      userId = inactivity.userId
      time = inactivity.startTime
      duration = inactivity.duration
      end = time + duration
    } yield CCActivityClusterFragment(userId, time, duration, inactivitySessionName, end, rank + 1)
  }

  def activityClusterFold(acc: InactivitySessionAccumulator, event: CCActivityClusterFragment)
      : InactivitySessionAccumulator = {
    val (state, sessionClustersFragments) = acc
    val (lastInactivitySessionRank, lastInactivitySessionEnd) = state

    event match {
      case CCActivityClusterFragment(userId, time, duration, `inactivitySessionName`, inactivitySessionsEnd, rank) =>
        val newState = (rank, inactivitySessionsEnd)
        (newState, sessionClustersFragments)
      case CCActivityClusterFragment(userId, time, duration, name, _, _) =>
        val newFragment =
          CCActivityClusterFragment(userId, time, duration, name, lastInactivitySessionEnd, lastInactivitySessionRank)
        (state, newFragment :: sessionClustersFragments)
      case _ => (state, sessionClustersFragments)
    }
  }

  def usersSessionsAndBreaksToActivityClusterFragments(sessionsAndBreaks: Iterable[CCActivityClusterFragment])
      : List[CCActivityClusterFragment] = {
    val sortedSessions = sessionsAndBreaks.toList.sortBy(_.time)
    val startTime = sortedSessions.head.time
    val startingState: InactivitySessionState = (0, startTime)
    val foldEmptyAcc: InactivitySessionAccumulator = (startingState, Nil)
    val (_, phoneSessions) = sortedSessions.foldLeft(foldEmptyAcc)(activityClusterFold)
    phoneSessions
  }

  def toActivityClusterFragments(events: RDD[CCActivityClusterFragment]): RDD[CCActivityClusterFragment] = {
    events.groupBy(_.userId).values
      .flatMap(usersSessionsAndBreaksToActivityClusterFragments)
  }
}

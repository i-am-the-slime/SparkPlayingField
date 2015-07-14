package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.EventType._
import org.menthal.model.Granularity._
import org.menthal.model.events.Implicits._
import org.menthal.model.events._
import org.menthal.spark.SparkHelper._

/**
 * Created by konrad on 20/05/15.
 */
object PhoneSessionsAggregation {

  val name: String = "PhoneSessionsAggregation"



  def main(args: Array[String]) {
    val (master, datadir) = args match {
      case Array(m, d) => (m,d)
      case _ =>
        val errorMessage = "First argument is master, second input/output path"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, name)
    //val sc = getSparkContext(master, name, Map("spark.akka.frameSize" -> "30"))
    //aggregate(sc, datadir)
    parquetToPhoneSessions(sc, datadir)
    sc.stop()
  }

  val phoneSessionsPath:String =  "/phone_sessions"
  val phoneInactiveSessionsPath:String = "/phone_inactivity_sessions"

  case class CCPhoneSessionFragment(userId: Long, sessionStart: Long, time: Long, duration: Long, app: String)
  case class CCPhoneInactiveSession(userId: Long, time: Long, duration: Long)

  def parquetToPhoneSessions(sc: SparkContext, datadir: String): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val appSession: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_APP_SESSION).map(toCCAppSession)
    val screenOff: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SCREEN_OFF).map(toCCScreenOff)
    val screenUnlock: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SCREEN_UNLOCK).map(toCCScreenUnlock)
    val dreamingStarted: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_DREAMING_STARTED).map(toCCDreamingStarted)
    val callMissed: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_CALL_MISSED).map(toCCCallMissed)
    val callReceived: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_CALL_RECEIVED).map(toCCCallReceived)
    val callOutgoing: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_CALL_OUTGOING).map(toCCCallOutgoing)
    val smsReceived: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SMS_RECEIVED).map(toCCSmsReceived)
    val smsSent: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SMS_SENT).map(toCCSmsSent)

    val processedEvents = appSession ++ screenOff ++ screenUnlock ++ dreamingStarted ++
       callMissed ++ callOutgoing ++ callReceived ++ smsReceived ++ smsSent

    val phoneSessionsFragmentsGroupedByUsers = eventsToPhoneSessions(processedEvents).cache()
    val phoneSessionFragments:RDD[CCPhoneSessionFragment] = phoneSessionsFragmentsGroupedByUsers.flatMap(x => x)
    val phoneInactiveSessions = phoneSessionsFragmentsGroupedByUsers.flatMap(phoneSessionsToPhoneInactiveSessions)
    phoneSessionFragments.toDF().saveAsParquetFile(datadir + phoneSessionsPath)
    phoneInactiveSessions.toDF().saveAsParquetFile(datadir + phoneInactiveSessionsPath)

  }

  type PhoneSessionState = (Long, Boolean)
  type PhoneSessionAccumulator = (PhoneSessionState, List[CCPhoneSessionFragment])
  type PhoneInactiveState = (Long, Long)
  type PhoneInactiveSessionAccumulator = (PhoneInactiveState, List[CCPhoneInactiveSession])

  def phoneSessionsFold(acc: PhoneSessionAccumulator, event: MenthalEvent): PhoneSessionAccumulator = {
    val (state, sessions) = acc
    val (currentSessionStart, screenUnlocked) = state
    if (screenUnlocked) event match {
      case CCAppSession(userId, time, duration, packageName) =>
        (state, CCPhoneSessionFragment(userId, currentSessionStart, time, duration, packageName) :: sessions)
      case CCCallMissed(_, userId, _, _, timestamp) =>
        (state, CCPhoneSessionFragment(userId, currentSessionStart, timestamp, 0, "call_missed") :: sessions)
      case CCCallOutgoing(_, userId, _, _, timestamp, duration) =>
        (state, CCPhoneSessionFragment(userId, currentSessionStart, timestamp, duration, "call_outgoing") :: sessions)
      case CCCallReceived(_, userId, _, _, timestamp, duration) =>
        (state, CCPhoneSessionFragment(userId, currentSessionStart, timestamp, duration, "call_received") :: sessions)
      case CCSmsReceived(_, userId, time, _, _) =>
        (state, CCPhoneSessionFragment(userId, currentSessionStart, time, 0, "sms_received") :: sessions)
      case CCSmsSent(_, userId, time, _, _) =>
        (state, CCPhoneSessionFragment(userId, currentSessionStart, time, 0, "sms_sent") :: sessions)
      case CCScreenOff(_, _, time) => ((time, false), sessions)
      case CCPhoneShutdown(_, _, time) => ((time, false), sessions)
      case CCDreamingStarted(_, _,time) => ((time, false), sessions)
      case _ => (state, sessions)
    } else event match {
      case CCScreenUnlock(_, userId, time) => ((time, true), sessions)
      case _ => (state, sessions)
    }
  }


  val startingPhoneSessionState: PhoneSessionState = (0, false)
  val foldEmptyAcc: PhoneSessionAccumulator = (startingPhoneSessionState, Nil)

  def transformToPhoneSessions(events: Iterable[_ <: MenthalEvent]): List[CCPhoneSessionFragment] = {
    val sortedEvents = events.toList.sortBy(_.time)
    val (_, phoneSessions) = sortedEvents.foldLeft(foldEmptyAcc)(phoneSessionsFold)
    phoneSessions
  }

  def eventsToPhoneSessions(events: RDD[_ <: MenthalEvent]): RDD[List[CCPhoneSessionFragment]] = {
      events.map { e => (e.userId, e) }
      .groupByKey()
      .values
      .map(transformToPhoneSessions)
  }


  def phoneInactiveSessionFold(acc: PhoneInactiveSessionAccumulator, session: CCPhoneSessionFragment) = {
    val (state, inactivitySessions) = acc
    val (currentSessionStart, currentSessionEnd) = state
    session match {
      case CCPhoneSessionFragment(userId, `currentSessionStart`, time, duration, _) =>
        val newState = (currentSessionStart, time + duration)
        (newState, inactivitySessions)
      case CCPhoneSessionFragment(userId, newSessionStart, time, duration, _) =>
        val newState = (newSessionStart, time + duration)
        val newInactivitySession = CCPhoneInactiveSession(userId, currentSessionEnd, newSessionStart - currentSessionStart)
        (newState, newInactivitySession :: inactivitySessions)
      case _ => (state, inactivitySessions)
    }
  }

  def phoneSessionsToPhoneInactiveSessions(phoneSessions: List[CCPhoneSessionFragment]) = {
    val sortedPhoneSessions = phoneSessions.sortBy(_.time)
    val startingPhoneInactiveState: PhoneInactiveState = (0, 0)
    val acc: PhoneInactiveSessionAccumulator = (startingPhoneInactiveState, Nil)
    val (state, inactivitySessions) = sortedPhoneSessions.foldLeft(acc)(phoneInactiveSessionFold)
    inactivitySessions
  }

}



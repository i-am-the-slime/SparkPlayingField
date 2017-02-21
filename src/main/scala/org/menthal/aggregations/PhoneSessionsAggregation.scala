package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.menthal.aggregations.tools.EventTransformers
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
    parquetToPhoneSessions(sc, datadir)
    phoneInactivitySessionsOnly(sc, datadir)
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
    val phoneInactiveSessions = phoneSessionsFragmentsGroupedByUsers.
                                  filter(!_.isEmpty).
                                  flatMap(phoneSessionsToPhoneInactiveSessions)
    phoneSessionFragments.toDF().saveAsParquetFile(datadir + phoneSessionsPath)
    phoneInactiveSessions.toDF().saveAsParquetFile(datadir + phoneInactiveSessionsPath)

  }

//  def parquetToPhoneSessions(sc: SparkContext, datadir: String): Unit = {
//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    val appSession: Dataset[MenthalEvent] = ParquetIO.readEventTypeToDF(sc, datadir, TYPE_APP_SESSION).as[AppSession]
//    val screenOff: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SCREEN_OFF).map(toCCScreenOff)
//    val screenUnlock: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SCREEN_UNLOCK).map(toCCScreenUnlock)
//    val dreamingStarted: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_DREAMING_STARTED).map(toCCDreamingStarted)
//    val callMissed: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_CALL_MISSED).map(toCCCallMissed)
//    val callReceived: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_CALL_RECEIVED).map(toCCCallReceived)
//    val callOutgoing: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_CALL_OUTGOING).map(toCCCallOutgoing)
//    val smsReceived: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SMS_RECEIVED).map(toCCSmsReceived)
//    val smsSent: RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SMS_SENT).map(toCCSmsSent)
//
//    val processedEvents = appSession ++ screenOff ++ screenUnlock ++ dreamingStarted ++
//      callMissed ++ callOutgoing ++ callReceived ++ smsReceived ++ smsSent
//
//    val phoneSessionsFragmentsGroupedByUsers = eventsToPhoneSessions(processedEvents).cache()
//    val phoneSessionFragments:RDD[CCPhoneSessionFragment] = phoneSessionsFragmentsGroupedByUsers.flatMap(x => x)
//    val phoneInactiveSessions = phoneSessionsFragmentsGroupedByUsers.
//      filter(!_.isEmpty).
//      flatMap(phoneSessionsToPhoneInactiveSessions)
//    phoneSessionFragments.toDF().saveAsParquetFile(datadir + phoneSessionsPath)
//    phoneInactiveSessions.toDF().saveAsParquetFile(datadir + phoneInactiveSessionsPath)
//
//  }

  def phoneInactivitySessionsOnly(sc: SparkContext, datadir: String): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //val phoneSessionFragments:RDD[CCPhoneSessionFragment] = phoneSessionsFragmentsGroupedByUsers.flatMap(x => x)
    val phoneSessionFragmentsDF = sqlContext.parquetFile(datadir + phoneSessionsPath)
    val phoneSessionFragments = for {
      row <- phoneSessionFragmentsDF
      userId = row.getLong(0)
      sessionStart = row.getLong(1)
      time = row.getLong(2)
      duration = row.getLong(3)
      app = "" // we don't care about apps
    } yield CCPhoneSessionFragment(userId, sessionStart, time, duration, app)
    val phoneSessionsFragmentsGroupedByUsers = phoneSessionFragments.groupBy(_.userId).values.map(_.toList)
    val phoneInactiveSessions = phoneSessionsFragmentsGroupedByUsers.flatMap(phoneSessionsToPhoneInactiveSessions)
    phoneInactiveSessions.toDF().saveAsParquetFile(datadir + phoneInactiveSessionsPath)
  }

  type PhoneSessionState = (Long, Long, Boolean)
  type PhoneSessionAccumulator = (PhoneSessionState, List[CCPhoneSessionFragment])
  type PhoneInactiveState = (Long, Long)
  type PhoneInactiveSessionAccumulator = (PhoneInactiveState, List[CCPhoneInactiveSession])

  val sessionLengthCutoff = 3 * 3600 * 1000 //3h in millis
  val beetweenAppSessionsTimeCutoff = 10 * 1000 //10s in millis

  def didSessionChange(lastSessionStart: Long, lastSessionEnd: Long, event: MenthalEvent):Boolean = {
    if ((event.time - lastSessionStart > sessionLengthCutoff)
      || (event.time - lastSessionEnd > beetweenAppSessionsTimeCutoff))
      true
    else
      false
  }

  def phoneSessionsFold(acc: PhoneSessionAccumulator, event: MenthalEvent): PhoneSessionAccumulator = {
    val (state, sessions) = acc
    val (lastSessionStart, lastSessionEnd, screenUnlocked) = state
    //We have to update state if there was change of phone session we might have skipped
    //Otherwise we suppose we are still in a same session

    val currentSessionStart = if (didSessionChange(lastSessionStart, lastSessionEnd, event)) event.time
                              else lastSessionStart
    val currentSessionEnd = event.time + EventTransformers.getDuration(event)
    val newStateUnlocked = (currentSessionStart, currentSessionEnd, true)
    val newStateLocked = (currentSessionStart, currentSessionEnd, false)

    if (screenUnlocked || didSessionChange(lastSessionStart, lastSessionEnd, event))
      event match {
      //Phone session fragment generating events
      case CCAppSession(userId, time, duration, packageName) =>
        (newStateUnlocked, CCPhoneSessionFragment(userId, currentSessionStart, time, duration, packageName) :: sessions)
      case CCCallMissed(_, userId, _, _, timestamp) =>
        (newStateUnlocked, CCPhoneSessionFragment(userId, currentSessionStart, timestamp, 0, "call_missed") :: sessions)
      case CCCallOutgoing(_, userId, _, _, timestamp, duration) =>
        (newStateUnlocked, CCPhoneSessionFragment(userId, currentSessionStart, timestamp, duration, "call_outgoing") :: sessions)
      case CCCallReceived(_, userId, _, _, timestamp, duration) =>
        (newStateUnlocked, CCPhoneSessionFragment(userId, currentSessionStart, timestamp, duration, "call_received") :: sessions)
      case CCSmsReceived(_, userId, time, _, _) =>
        (newStateUnlocked, CCPhoneSessionFragment(userId, currentSessionStart, time, 0, "sms_received") :: sessions)
      case CCSmsSent(_, userId, time, _, _) =>
        (newStateUnlocked, CCPhoneSessionFragment(userId, currentSessionStart, time, 0, "sms_sent") :: sessions)
      //Locking events
      case CCScreenOff(_, _, time) => (newStateLocked, sessions)
      case CCPhoneShutdown(_, _, time) => (newStateLocked, sessions)
      case CCDreamingStarted(_, _,time) => (newStateLocked, sessions)
      //Shouldn't happen but in case we just don't change anything
      case _ => (newStateUnlocked, sessions)

    } else event match {
      case CCScreenUnlock(_, userId, time) => (newStateUnlocked, sessions)
      case _ => (newStateLocked, sessions)
    }
  }



  def transformToPhoneSessions(events: Iterable[_ <: MenthalEvent]): List[CCPhoneSessionFragment] = {
    val sortedEvents = events.toList.sortBy(_.time)
    val foldStartingState:PhoneSessionState =  (sortedEvents.head.time, sortedEvents.head.time, false)
    val foldStartingAcc:PhoneSessionAccumulator = (foldStartingState, Nil)
    val (_, phoneSessions) = sortedEvents.foldLeft(foldStartingAcc)(phoneSessionsFold)
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
    val (lastSessionStart, lastSessionEnd) = state
    session match {
      case CCPhoneSessionFragment(userId, newSessionStart, time, duration, _) =>
        val newState = (newSessionStart, time + duration)
        if (newSessionStart == lastSessionStart) {
          (newState, inactivitySessions)
        }
        else {
          val newInactivitySession = CCPhoneInactiveSession(userId, lastSessionEnd, newSessionStart - lastSessionEnd)
          (newState, newInactivitySession :: inactivitySessions)
        }
      case _ => (state, inactivitySessions)
    }
  }

  def phoneSessionsToPhoneInactiveSessions(phoneSessions: List[CCPhoneSessionFragment]) = {
    val sortedPhoneSessions = phoneSessions.sortBy(_.time)
    val startingPhoneInactiveState: PhoneInactiveState = (sortedPhoneSessions.head.time, sortedPhoneSessions.head.time)
    val acc: PhoneInactiveSessionAccumulator = (startingPhoneInactiveState, Nil)
    val (_, inactivitySessions) = sortedPhoneSessions.foldLeft(acc)(phoneInactiveSessionFold)
    inactivitySessions
  }

}



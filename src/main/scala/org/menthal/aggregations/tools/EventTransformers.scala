package org.menthal.aggregations.tools

import java.util.{List => JList, Map => JMap}
import com.twitter.algebird.Operators._
import org.joda.time.DateTime
import org.menthal.model.Granularity
import org.menthal.model.Granularity.{TimePeriod, Timestamp}
import org.menthal.model.events._
import org.menthal.model.implicits.DateImplicits._

import scala.annotation.tailrec
import scala.collection.mutable.{Map => MMap}

/**
 * Created by konrad on 21.07.2014.
 */

case class SimpleMenthalEvent(val userId: Long, val time: Long, val key: String, val value: Long)

object EventTransformers {



  def eventAsKeyValuePairs(event: MenthalEvent): List[(String, Long)] = {
    event match {
      case e:CCAppSession ⇒ List((e.packageName, e.duration))
      case e:CCCallMissed ⇒ List((e.contactHash, 0L))
      case e:CCCallOutgoing ⇒ List((e.contactHash, e.durationInMillis.toLong))
      case e:CCCallReceived ⇒ List((e.contactHash, e.durationInMillis.toLong))
      case e:CCSmsSent ⇒ List((e.contactHash, e.msgLength.toLong))
      case e:CCSmsReceived ⇒ List((e.contactHash, e.msgLength.toLong))
      case e:CCWhatsAppReceived ⇒ List((e.contactHash, e.messageLength.toLong))
      case e:CCWhatsAppSent ⇒ List((e.contactHash, e.messageLength.toLong))
      case _ => List()
    }
  }

  //TODO think about case when keys are not unique
  def eventAsMap(event: MenthalEvent): Map[String, Long] =
    eventAsKeyValuePairs(event).map{ case (k,v) => Map(k->v)}.fold(Map[String,Long]())(_ + _)

  def eventAsCounter(event: MenthalEvent): Map[String, Long] =
    eventAsKeyValuePairs(event).map{ case (k,_) => Map(k -> 1L)}.fold(Map[String,Long]())(_ + _)

  def getKeyFromEvent(event: MenthalEvent) : String = event match {
    case e:CCAppSession ⇒ e.packageName
    case e:CCCallReceived ⇒ e.contactHash
    case e:CCCallMissed ⇒ e.contactHash
    case e:CCCallOutgoing ⇒ e.contactHash
    case e:CCNotificationStateChanged ⇒ e.packageName
    case e:CCScreenOn ⇒ "screen_on"
    case e:CCScreenOff ⇒ "screen_off"
    case e:CCScreenUnlock ⇒ "screen_unlock"
    case e:CCSmsReceived ⇒ e.contactHash
    case e:CCSmsSent ⇒ e.contactHash
    case e:CCWhatsAppReceived ⇒ e.contactHash
    case e:CCWhatsAppSent ⇒ e.contactHash
    case _ ⇒ "unknown"
    //TODO complete this function????
  }

//  def getKeyNameFromEvent(event: MenthalEvent) : String = event match {
//    case e:CCAppSession ⇒ "apps"
//    case e:CCCallReceived | CCCallMissed | CallOutgoing ⇒ "call_participant"
//    case e:CCNotificationStateChanged  ⇒ "apps"
//    case e:CCScreenOn ⇒ "screen_on"
//    case e:CCScreenOff ⇒ "screen_off"
//    case e:CCScreenUnlock ⇒ "screen_unlock"
//    case e:CCSmsReceived | SmsSent ⇒ "sms_participant"
//    case e:CCWhatsAppReceived ⇒ "whatsapp_participant"
//    case _ ⇒ "unknown"
//    //TODO complete this function????
//  }

  def getDuration(event: MenthalEvent): Long = event match {
      case e:CCCallReceived => e.durationInMillis
      case e:CCCallOutgoing => e.durationInMillis
      case e:CCAppSession => e.duration
      case _ => 0
    }


  def getMessageLength(event: MenthalEvent): Long = event match {
      case e:CCSmsReceived => e.msgLength
      case e:CCSmsSent => e.msgLength
      case e:CCWhatsAppReceived => e.messageLength
      case e:CCWhatsAppSent => e.messageLength
      case _ => 0
  }


  def getValueFromEvent(event:MenthalEvent): Long =
   event match {
    case e:CCCallReceived => e.durationInMillis
    case e:CCCallOutgoing => e.durationInMillis
    case e:CCAppSession => e.duration
    case e:CCSmsReceived => e.msgLength
    case e:CCSmsSent => e.msgLength
    case e:CCWhatsAppReceived => e.messageLength
    case e:CCWhatsAppSent => e.messageLength
    case _ => 0
  }

  def getSplittingTime(start: Timestamp, durationInMillis: Long, timePeriod: TimePeriod): List[(Timestamp, Long)] = {
    @tailrec
    def go(start: Timestamp, durationInMillis: Long, result:List[(Timestamp, Long)]):List[(Timestamp, Long)] = {
      val endTimeRounded   = Granularity.roundTimestamp(start + durationInMillis, timePeriod)
      val startTimeRounded = Granularity.roundTimestamp(start, timePeriod)
      if (endTimeRounded > startTimeRounded) {
        val newStart = Granularity.roundTimestampCeiling(start, timePeriod)
        val currentDuration = newStart - start
        val newDuration = durationInMillis - currentDuration
        go(newStart, newDuration, (start, currentDuration) :: result)
      } else (start, durationInMillis) :: result
    }
    go(start, durationInMillis, Nil)
  }

  def splitEventByRoundedTime(event: MenthalEvent, timePeriod: TimePeriod): List[_ <: MenthalEvent] = {
    event match {
      case e: CCAppSession ⇒
        for ((start, duration) ← getSplittingTime(e.time, e.duration, timePeriod))
          yield e.copy(time = start, duration = duration)

      case e: CCCallOutgoing ⇒
        for ((start, duration) ← getSplittingTime(e.time, e.durationInMillis, timePeriod))
          yield e.copy(time = start, startTimestamp = start, durationInMillis = duration)

      case e: CCCallReceived ⇒
        for ((start, duration) ← getSplittingTime(e.startTimestamp, e.durationInMillis, timePeriod))
          yield e.copy(time = start, startTimestamp = start, durationInMillis = duration)

      case _ ⇒ List(event)
    }
  }

  def splitEventToTuplesByRoundedTime[T <: MenthalEvent](event: T, timePeriod: TimePeriod): List[SimpleMenthalEvent] = {
    event match {
      case e: CCAppSession ⇒
        for ((start, duration) ← getSplittingTime(e.time, e.duration, timePeriod))
          yield SimpleMenthalEvent(e.userId, start, getKeyFromEvent(e), duration)

      case e: CCCallOutgoing ⇒
        for ((start, duration) ← getSplittingTime(e.time, e.durationInMillis, timePeriod))
          yield SimpleMenthalEvent(e.userId, start, getKeyFromEvent(e), duration)

      case e: CCCallReceived ⇒
        for ((start, duration) ← getSplittingTime(e.startTimestamp, e.durationInMillis, timePeriod))
          yield SimpleMenthalEvent(e.userId, start, getKeyFromEvent(e), duration)

      case e ⇒ List(SimpleMenthalEvent(e.userId, e.time, getKeyFromEvent(e), getValueFromEvent(e)))
    }
  }
}
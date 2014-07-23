package org.menthal.model.events

import org.joda.time.{Hours, DateTime}
import org.menthal.model.implicits.EventImplicts._
import com.twitter.algebird.Operators._

/**
 * Created by konrad on 21.07.2014.
 */
object MenthalUtils {

  def eventAsKeyValuePars(event: MenthalEvent): List[(String, Long)] = {
    event match {
      case CCSmsReceived(_, _, _, contactHash, msgLength) => List((contactHash, msgLength.toLong))
      case CCCallMissed(_, _, _, contactHash, _) => List((contactHash, 0L))
      case CCCallOutgoing(_, _, _, contactHash, _, duration) => List((contactHash, duration))
      case CCCallReceived(_, _, _, contactHash, _, duration) => List((contactHash, duration))
      case CCSmsSent(_, _, _, contactHash, msgLength) => List((contactHash, msgLength.toLong))
      case CCWhatsAppReceived(_, _, _, contactHash, msgLength, _) => List((contactHash, msgLength.toLong))
      case CCWhatsAppSent(_, _, _, contactHash, msgLength,_) => List((contactHash, msgLength.toLong))
      case _ => List()
    }
  }

  //TODO think about case when keys are not unique
  def eventAsMap(event: MenthalEvent): Map[String, Long] = {
    eventAsKeyValuePars(event) toMap
  }

  def eventAsCounter(event: MenthalEvent): Map[String, Int] = {
    eventAsKeyValuePars(event) map { kv => Map(kv._1 -> 1)} reduce(_ + _)
  }

  def roundTime(time: DateTime): DateTime = {
    time.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
  }

  def roundTimeCeiling(time: DateTime): DateTime = {
    time.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).plusHours(1)
  }

  def getDuration(event: MenthalEvent): Long = {
    event match {
      case CCCallReceived(_,_,_,_,_,durationInMillis) => durationInMillis
      case _ => 0
    }
  }

  def getSplittingTime(start: DateTime, duration: Long): List[(DateTime, Long)] = {
    if (roundTime(new DateTime(start + duration)) > roundTime(start)) {
      val new_start = roundTimeCeiling(new DateTime(start + duration))
      val newDuration = new_start - start
      (start, newDuration) :: getSplittingTime(new_start, duration - newDuration)
    }
    else
      List((start, duration))
  }

  def splitEventByRoundedTime(event: MenthalEvent): List[MenthalEvent] = {
      event match {
        case CCCallReceived(id, userId, time, contactHash, startTimestamp, durationInMillis) => {
          for ((start, duration) <- getSplittingTime(new DateTime(time), durationInMillis))
          yield CCCallReceived(id, userId, start, contactHash, start, duration)
        }
        case _ => List(event)
      }
    }

}
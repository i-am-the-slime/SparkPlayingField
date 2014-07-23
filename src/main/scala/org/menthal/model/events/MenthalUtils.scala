package org.menthal.model.events

import org.joda.time.{Hours, DateTime}
import org.menthal.model.implicits.EventImplicts._
import com.twitter.algebird.Operators._

import scala.annotation.tailrec

/**
 * Created by konrad on 21.07.2014.
 */
object MenthalUtils {

  def eventAsKeyValuePairs(event: MenthalEvent): List[(String, Long)] = {
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
  def eventAsMap(event: MenthalEvent): Map[String, Long] =
    eventAsKeyValuePairs(event).map{ case (k,v) => Map(k -> v)}.foldLeft(Map[String, Long]())(_ + _)

  def eventAsCounter(event: MenthalEvent): Map[String, Int] =
    eventAsKeyValuePairs(event).map{ case (k,_) => Map(k -> 1)}.foldLeft(Map[String, Int]())(_ + _)

  def roundTime(time: DateTime): DateTime =
    time.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)

  def roundTimeCeiling(time: DateTime): DateTime =
    roundTime(time).plusHours(1)

  //TODO complete this function
  def getDuration(event: MenthalEvent): Long = {
    event match {
      case CCCallReceived(_,_,_,_,_,durationInMillis) => durationInMillis
      case _ => 0
    }
  }

  def getSplittingTime(start: DateTime, durationInMillis: Long): List[(DateTime, Long)] = {
    if (roundTime(new DateTime(start + durationInMillis)) > roundTime(start)) {
      val newStart = roundTimeCeiling(new DateTime(start))
      val newDuration = newStart - start
      (start, newDuration) :: getSplittingTime(newStart, durationInMillis - newDuration)
    }
    else
      List((start, durationInMillis))
  }

  def splitEventByRoundedTime(event: MenthalEvent): List[MenthalEvent] = {
      event match {
        case CCCallReceived(id, userId, time, contactHash, startTimestamp, durationInSeconds) => {
          for ((start, duration) <- getSplittingTime(new DateTime(startTimestamp), durationInSeconds))
          yield CCCallReceived(id, userId, start, contactHash, start, duration)
        }
        case _ => List(event)
      }
    }

}
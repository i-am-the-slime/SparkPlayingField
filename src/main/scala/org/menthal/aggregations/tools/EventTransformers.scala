package org.menthal.aggregations.tools

import java.util.{List => JList, Map => JMap}
import com.twitter.algebird._
import com.twitter.algebird.Operators._
import org.joda.time.DateTime
import org.menthal.model.Granularity
import org.menthal.model.events._
import org.menthal.model.implicits.DateImplicits._

import scala.collection.mutable.{Map => MMap}

/**
 * Created by konrad on 21.07.2014.
 */
object EventTransformers {

  def eventAsKeyValuePairs(event: MenthalEvent): List[(String, Long)] = {
    event match {
      case CCSmsReceived(_, _, _, contactHash, msgLength) => List((contactHash, msgLength.toLong))
      case CCCallMissed(_, _, _, contactHash, _) => List((contactHash, 0L))
      case CCCallOutgoing(_, _, _, contactHash, _, duration) => List((contactHash, duration.toLong))
      case CCCallReceived(_, _, _, contactHash, _, duration) => List((contactHash, duration.toLong))
      case CCSmsSent(_, _, _, contactHash, msgLength) => List((contactHash, msgLength.toLong))
      case CCWhatsAppReceived(_, _, _, contactHash, msgLength, _) => List((contactHash, msgLength.toLong))
      case CCWhatsAppSent(_, _, _, contactHash, msgLength,_) => List((contactHash, msgLength.toLong))
      case _ => List()
    }
  }

  //TODO think about case when keys are not unique
  def eventAsMap(event: MenthalEvent): Map[String, Long] =
    eventAsKeyValuePairs(event).map{ case (k,v) => Map(k->v)}.fold(Map[String,Long]())(_ + _)

  def eventAsCounter(event: MenthalEvent): Map[String, Long] =
    eventAsKeyValuePairs(event).map{ case (k,_) => Map(k -> 1L)}.fold(Map[String,Long]())(_ + _)

  //TODO complete this function
  def getDuration(event: MenthalEvent): Long = {
    event match {
      case CCCallReceived(_,_,_,_,_,durationInMillis) => durationInMillis
      case _ => 0
    }
  }

  def getSplittingTime(start: DateTime, durationInMillis: Long): List[(DateTime, Long)] = {
    if (Granularity.roundTime(new DateTime(start + durationInMillis), Granularity.Hourly) > Granularity.roundTime(start, Granularity.Hourly)) {
      val newStart = Granularity.roundTimeCeiling(new DateTime(start), Granularity.Hourly)
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
          yield CCCallReceived(id, userId, dateToLong(start), contactHash, dateToLong(start), duration)
        }
        case _ => List(event)
      }
    }

}
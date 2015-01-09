package org.menthal.model

import org.joda.time.DateTime
import scalaz.Maybe
import scalaz.syntax.std.option._

/**
 * Created by konrad on 29.10.14.
 */

object Granularity {
  type TimePeriod = Int
  val Hourly:TimePeriod = 1
  val Daily:TimePeriod = 2
  val Weekly:TimePeriod = 3
  val Monthly:TimePeriod = 4
  val Yearly:TimePeriod = 5

  def roundTimeFloor(time: DateTime, timePeriod:TimePeriod): DateTime = {
    val base = time.withTimeAtStartOfDay()
    timePeriod match {
      case Hourly  ⇒  base withHourOfDay time.getHourOfDay
      case Daily   ⇒  base
      case Weekly  ⇒  base withDayOfWeek  0
      case Monthly ⇒  base withDayOfMonth 0
      case Yearly  ⇒  base withDayOfYear  0
    }
  }

  def asString(timePeriod:TimePeriod) = timePeriod match {
    case Hourly  ⇒ "Hourly"
    case Daily   ⇒ "Daily"
    case Weekly  ⇒ "Weekly"
    case Monthly ⇒ "Monthly"
    case Yearly  ⇒ "Yearly"
  }

  def sub(timePeriod:TimePeriod):Option[TimePeriod] = timePeriod match {
    case Daily   ⇒  None
    case Weekly  ⇒  Daily.some
    case Monthly ⇒  Daily.some
    case Yearly  ⇒  Monthly.some
    case _       ⇒  None
  }

  def roundTimeCeiling(time: DateTime, timePeriod:TimePeriod): DateTime = {
    val roundedDown = roundTimeFloor(time, timePeriod)
    timePeriod match {
      case Hourly  ⇒  roundedDown plusHours 1
      case Daily   ⇒  roundedDown plusDays 1
      case Weekly  ⇒  roundedDown plusWeeks 1
      case Monthly ⇒  roundedDown plusMonths 1
      case Yearly  ⇒  roundedDown plusYears 1
    }
  }
}


package org.menthal.model

import org.joda.time.DateTime

/**
 * Created by konrad on 29.10.14.
 */
object Granularity
  extends Enumeration {
  type Granularity = Value
  val Hourly, Daily, Weekly, Monthly, Yearly = Value


  def roundTime(time: DateTime, granularity: Granularity): DateTime = {
    granularity match {
      case Hourly =>
        time.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      case Daily =>
        time.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      case Weekly =>
        time.withDayOfWeek(0).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      case Monthly =>
        time.withDayOfMonth(0).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      case Yearly =>
        time.withDayOfYear(0).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
    }
  }

  def roundTimeCeiling(time: DateTime, granularity: Granularity): DateTime = {
    val rounded = roundTime(time, granularity)
    granularity match {
      case Hourly =>
        rounded.plusHours(1)
    }
  }
}

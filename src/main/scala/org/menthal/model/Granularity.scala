package org.menthal.model

import org.joda.time.DateTime

/**
 * Created by konrad on 29.10.14.
 */
object Granularity
  extends Enumeration {
  type Granularity = Value
  val Hourly, Daily, Weekly, Monthly, Yearly = Value

  def granularityToLong(granularity:Granularity):Long = granularity match {
    case Hourly => 1
    case Daily => 2
    case Weekly => 3
  }

  def roundTimeFloor(time: DateTime, granularity: Granularity): DateTime = {
    granularity match {
      case Hourly =>
        time.withTimeAtStartOfDay().withHourOfDay(time.getHourOfDay)
      case Daily =>
        time.withTimeAtStartOfDay()
      case Weekly =>
        time.withDayOfWeek(0).withTimeAtStartOfDay()
      case Monthly =>
        time.withDayOfMonth(0).withTimeAtStartOfDay()
      case Yearly =>
        time.withDayOfYear(0).withTimeAtStartOfDay()
    }
  }

  def roundTimeCeiling(time: DateTime, granularity: Granularity): DateTime = {
    val rounded = roundTimeFloor(time, granularity)
    granularity match {
      case Hourly =>
        rounded.plusHours(1)
    }
  }
}

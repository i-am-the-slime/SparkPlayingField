package org.menthal.model

import org.joda.time.DateTime
import scalaz.Maybe
import scalaz.syntax.std.option._

/**
 * Created by konrad on 29.10.14.
 */

object Granularity {
  type TimePeriod = Int
  type Timestamp = Long
  val FiveMin:TimePeriod = 6
  val FifteenMin:TimePeriod = 7
  val Hourly:TimePeriod = 1
  val Daily:TimePeriod = 2
  val Weekly:TimePeriod = 3
  val Monthly:TimePeriod = 4
  val Yearly:TimePeriod = 5

  val all:List[TimePeriod] = List(
    Granularity.FiveMin,
    Granularity.FifteenMin,
    Granularity.Hourly,
    Granularity.Daily,
    Granularity.Weekly,
    Granularity.Monthly,
    Granularity.Yearly)

  def roundTimeFloor(time: DateTime, timePeriod:TimePeriod): DateTime = {
    val base = time.withTimeAtStartOfDay()
    timePeriod match {
      case FiveMin =>  base withHourOfDay time.getHourOfDay withMinuteOfHour (time.getMinuteOfHour - time.getMinuteOfHour % 5)
      case FifteenMin =>  base withHourOfDay time.getHourOfDay withMinuteOfHour (time.getMinuteOfHour - time.getMinuteOfHour % 15)
      case Hourly  ⇒  base withHourOfDay time.getHourOfDay
      case Daily   ⇒  base
      case Weekly  ⇒  base withDayOfWeek  1
      case Monthly ⇒  base withDayOfMonth 1
      case Yearly  ⇒  base withDayOfYear  1
    }
  }

  def asString(timePeriod:TimePeriod) = timePeriod match {
    case FiveMin => "FiveMin"
    case FifteenMin => "FifteenMin"
    case Hourly  ⇒ "Hourly"
    case Daily   ⇒ "Daily"
    case Weekly  ⇒ "Weekly"
    case Monthly ⇒ "Monthly"
    case Yearly  ⇒ "Yearly"
  }

  def sub(timePeriod:TimePeriod):Option[TimePeriod] = timePeriod match {
    case Weekly  ⇒  Daily.some
    case Monthly ⇒  Daily.some
    case Yearly  ⇒  Monthly.some
    case _       ⇒  None
  }

  def roundTimeCeiling(time: DateTime, timePeriod:TimePeriod): DateTime = {
    val roundedDown = roundTimeFloor(time, timePeriod)
    timePeriod match {
      case FiveMin => roundedDown plusMinutes 5
      case FifteenMin => roundedDown plusMinutes 15
      case Hourly  ⇒  roundedDown plusHours 1
      case Daily   ⇒  roundedDown plusDays 1
      case Weekly  ⇒  roundedDown plusWeeks 1
      case Monthly ⇒  roundedDown plusMonths 1
      case Yearly  ⇒  roundedDown plusYears 1
    }
  }

  def millisPerMinute:Long = 60 * 1000
  def millisPerHour:Long = 60 * millisPerMinute
  def millisPerDay:Long = 24 * millisPerHour


  def durationInMillis(timePeriod:TimePeriod): Long =
    timePeriod match {
      case FiveMin => 5 * millisPerMinute
      case FifteenMin => 15 * millisPerMinute
      case Hourly  ⇒  millisPerHour
      case Daily   ⇒  millisPerDay
      case Weekly  ⇒  7 * millisPerDay
      case Monthly ⇒  30 * millisPerDay
      case Yearly  ⇒  365 * millisPerDay
    }

  def roundTimestamp(time: Long,  timePeriod:TimePeriod):Long = {
    val duration = durationInMillis(timePeriod)
    time - time % duration
  }

  def roundTimestampCeiling(time: Long,  timePeriod:TimePeriod): Long = {
    val roundedDown = roundTimestamp(time, timePeriod)
    val duration = durationInMillis(timePeriod)
    roundedDown + duration
  }

  type GranularityForest= Forest[TimePeriod]
  val fullGranularitiesForest: GranularityForest = List(
    Leaf(Granularity.FiveMin),
    Leaf(Granularity.FifteenMin),
    Node(Granularity.Hourly, List(
      Node(Granularity.Daily, List(
        Node(Granularity.Monthly, List(
          Leaf(Granularity.Yearly))),
        Leaf(Granularity.Weekly))))))

  type Forest[A] = List[Tree[A]]
  trait Tree[A] {
    def a: A
    def reductionsTree[B](z: B)(f: (B, A) => B): Tree[B] = {
      this match {
        case Leaf(x) => Leaf(f(z, x))
        case Node(x, children) => {
          val y = f(z, x)
          val newChildren = for (child <- children) yield child.reductionsTree(y)(f)
          Node(y, newChildren)
        }
      }
    }

    def traverseTree[B](z: B)(f: (B, A) => B): Unit = {
      this match {
        case Leaf(x) =>
          f(z, x)
        case Node(x, children) =>
          val y = f(z, x)
          for (child <- children) child.traverseTree(y)(f)
      }
    }
  }

  case class Leaf[A](a: A) extends Tree[A]
  case class Node[A](a: A, children: List[Tree[A]]) extends Tree[A]


}


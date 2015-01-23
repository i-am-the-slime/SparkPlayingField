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
      case Weekly  ⇒  base withDayOfWeek  1
      case Monthly ⇒  base withDayOfMonth 1
      case Yearly  ⇒  base withDayOfYear  1
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
  type GranularityForest= Forest[TimePeriod]
  val fullGranularitiesForest: GranularityForest = List(Leaf(Granularity.Hourly),
    Node(Granularity.Daily, List(
      Node(Granularity.Monthly, List(
        Leaf(Granularity.Yearly))),
      Leaf(Granularity.Weekly))))

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


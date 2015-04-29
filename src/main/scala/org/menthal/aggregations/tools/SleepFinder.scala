package org.menthal.aggregations.tools

import org.menthal.aggregations.SleepAggregations.{CCNoInteractionPeriod, CCSignalWindow}
import org.menthal.model.Granularity
import org.menthal.model.Granularity.TimePeriod

/**
 * Created by konrad on 16/04/15.
 */
object SleepFinder {

  def updateNoInteractionWindows(granularity: TimePeriod)(windows: List[CCNoInteractionPeriod], signal: CCSignalWindow): List[CCNoInteractionPeriod] = {
    signal match {
      case CCSignalWindow(userId, startTime, 0) => {
        val windowDuration = Granularity.durationInMillis(granularity)
        val endTime = startTime + windowDuration
        windows match {
          case x :: xs =>
            if (x.endTime == startTime) {
              x.copy(endTime = endTime, duration = x.duration + windowDuration) :: xs
            } else {
              val duration = endTime - startTime
              CCNoInteractionPeriod(userId, startTime, endTime, duration) :: xs
            }
          case Nil => List(CCNoInteractionPeriod(userId, startTime, endTime, windowDuration))
        }
      }
      case _ => windows
    }
  }

  def findLongest(longest: CCNoInteractionPeriod, window: CCNoInteractionPeriod):CCNoInteractionPeriod = {
    if ((window.startTime - window.endTime) > (longest.startTime - longest.endTime))
      window
    else
      longest
  }

  def findLongestNoInteractionWindow(windows: Iterable[CCNoInteractionPeriod]):CCNoInteractionPeriod = {
    windows.reduce(findLongest)
  }


  def findNoInteractionPeriods(signal: Iterable[CCSignalWindow], granularity: TimePeriod):Iterable[CCNoInteractionPeriod] = {
    signal.foldLeft[List[CCNoInteractionPeriod]](List())(updateNoInteractionWindows(granularity))
  }

  def findLongestSleepTimes(granularity: TimePeriod)(signal: Iterable[CCSignalWindow]):Iterable[CCNoInteractionPeriod] = {
    val noInteractionPeriods = findNoInteractionPeriods(signal, granularity)
    val groupedByDay = noInteractionPeriods
      .groupBy(period =>  Granularity.roundTimestamp(period.endTime, granularity))
      .values
    val longestWindows = groupedByDay map findLongestNoInteractionWindow
    longestWindows
  }
}

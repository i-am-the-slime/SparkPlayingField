package org.menthal.aggregations.tools

import org.menthal.aggregations.SleepAggregations.{CCNoInteractionPeriod, CCSignalWindow}
import org.menthal.model.Granularity
import org.menthal.model.Granularity.TimePeriod

/**
 * Created by konrad on 16/04/15.
 */
object SleepFinder {

  def updateNoInteractionWindows(windowGranularity: TimePeriod)(windows: List[CCNoInteractionPeriod], signal: CCSignalWindow): List[CCNoInteractionPeriod] = {
    signal match {
      case CCSignalWindow(userId, startTime, 0) => {
        val windowDuration = Granularity.durationInMillis(windowGranularity)
        val endTime = startTime + windowDuration
        windows match {
          case CCNoInteractionPeriod(userId, lastStartTime, `startTime`, lastWindowDuration) :: xs =>
              CCNoInteractionPeriod(userId, lastStartTime, endTime, lastWindowDuration + windowDuration) :: xs
          case _ => CCNoInteractionPeriod(userId, startTime, endTime, windowDuration) :: windows
        }
      }
      case _ => windows
    }
  }

  def findNoInteractionPeriods(windowGranularity: TimePeriod)(signal: Iterable[CCSignalWindow]):Iterable[CCNoInteractionPeriod] = {
    val sortedSignal = signal.toList.sortWith(_.timeWindow < _.timeWindow)
    def noInteractionsFolder = updateNoInteractionWindows(windowGranularity) _
    sortedSignal.foldLeft[List[CCNoInteractionPeriod]](Nil)(noInteractionsFolder)
  }

  def findDailySleep(noInteractionPeriods: Iterable[CCNoInteractionPeriod]):Iterable[CCNoInteractionPeriod] = {
    def findLongest(longest: CCNoInteractionPeriod, window: CCNoInteractionPeriod):CCNoInteractionPeriod =
      if ((window.duration) > (longest.duration)) window else longest
    val groupedByDay = noInteractionPeriods
      .groupBy(period => Granularity.roundTimestamp(period.endTime, Granularity.Daily))
      .values
    val longestWindowsByDays = groupedByDay map { _.reduce(findLongest)}
    longestWindowsByDays
  }

  def findDailySleepTimeFromSignal(windowGranularity: TimePeriod)(signal: Iterable[CCSignalWindow]):Iterable[CCNoInteractionPeriod] = {
    val noInteractionPeriods = findNoInteractionPeriods(windowGranularity)(signal)
    findDailySleep(noInteractionPeriods)
  }


}

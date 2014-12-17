package org.menthal

import org.joda.time.DateTime
import org.menthal.aggregations.tools.EventTransformers
import org.menthal.model.Granularity
import Granularity._
import org.menthal.model.events._
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mark on 23.07.14.
 */
class MenthalUtilsSpec extends FlatSpec with Matchers{
  "eventAsKeyValuePairs" should "transform MenthalEvents to Lists" in {
    EventTransformers.eventAsKeyValuePairs(CCSmsReceived(1,2,3, "conha", 5)) shouldBe List(("conha", 5L))
    EventTransformers.eventAsKeyValuePairs(CCCallMissed(1,2,3, "hash", 4)) shouldBe List(("hash", 0L))
    EventTransformers.eventAsKeyValuePairs(CCCallOutgoing(1,2,3, "hash", 4, 5)) shouldBe List(("hash", 5L))
    EventTransformers.eventAsKeyValuePairs(CCCallReceived(1,2,3, "hash", 4, 5)) shouldBe List(("hash", 5L))
    EventTransformers.eventAsKeyValuePairs(CCSmsSent(1,2,3, "hash", 4)) shouldBe List(("hash", 4L))
    EventTransformers.eventAsKeyValuePairs(CCSmsReceived(1,2,3, "hash", 4)) shouldBe List(("hash", 4L))
    EventTransformers.eventAsKeyValuePairs(CCWhatsAppSent(1,2,3, "hash", 4, isGroupMessage = false))  shouldBe List(("hash", 4L))
    EventTransformers.eventAsKeyValuePairs(CCWhatsAppReceived(1,2,3, "hash", 4, isGroupMessage = true)) shouldBe List(("hash", 4L))
  }

  it should "return an empty list for other MenthalEvents" in {
    EventTransformers.eventAsKeyValuePairs(CCDreamingStarted(1,2,3)) shouldBe List()
    EventTransformers.eventAsKeyValuePairs(CCAppInstall(1,2,3,"balls", "packageName")) shouldBe List()
  }

  "eventAsMap" should "turn MenthalEvents into Maps" in {
    EventTransformers.eventAsMap(CCCallMissed(1,2,3, "hash", 4)) shouldBe Map("hash" -> 0L)
    EventTransformers.eventAsMap(CCAppInstall(1,2,3,"balls", "packageName")) shouldBe Map()
  }

  "eventAsCounter" should "count the number of events" in {
    EventTransformers.eventAsCounter(CCCallMissed(1,2,3, "hash", 4)) shouldBe Map("hash" -> 1)
    EventTransformers.eventAsCounter(CCAppInstall(1,2,3,"balls", "packageName")) shouldBe Map()
  }

  "roundTime" should "time at the previous full hours" in {
    val date = DateTime.parse("2014-01-01T21:58:44.752+01")
    val correctDate = DateTime.parse("2014-01-01T21:00:00.000+01")
    Granularity.roundTimeFloor(date, Hourly) shouldBe correctDate

    val date2 = DateTime.parse("2014-01-01T23:05:03.752+01")
    val correctDate2 = DateTime.parse("2014-01-01T23:00:00.000+01")
    Granularity.roundTimeFloor(date2, Hourly) shouldBe correctDate2
  }

  "roundTimeCeiling" should "time at the next full hours" in {
    val date = DateTime.parse("2014-01-01T21:58:44.752+01")
    val correctDate = DateTime.parse("2014-01-01T22:00:00.000+01")
    Granularity.roundTimeCeiling(date, Hourly) shouldBe correctDate

    val date2 = DateTime.parse("2014-01-01T23:05:03.752+01")
    val correctDate2 = DateTime.parse("2014-01-02T00:00:00.000+01")
    Granularity.roundTimeCeiling(date2, Hourly) shouldBe correctDate2

    val date3 = DateTime.parse("2016-02-28T23:05:03.752+01")
    val correctDate3 = DateTime.parse("2016-02-29T00:00:00.000+01")
    Granularity.roundTimeCeiling(date3, Hourly) shouldBe correctDate3
  }

  val date = DateTime.parse("2014-01-01T21:58:44.752+01")
  val nextDate = DateTime.parse("2014-01-01T22:00:00.000+01")
  val endDate = DateTime.parse("2014-01-01T23:00:00.000+01")
  val correct = List(
    (date, 75248),
    (nextDate, 3524752)
  )
  val correct2 = List(
    (date, 75248),
    (nextDate, 3600000),
    (endDate, 3524752)
  )

  "getSplittingTime" should "yield durations that span over full hours should be split" in {
    EventTransformers.getSplittingTime(date, 3600000) shouldBe correct
    EventTransformers.getSplittingTime(date, 7200000) shouldBe correct2
    EventTransformers.getSplittingTime(date, 1) shouldBe List((date, 1))
  }

  "splitEventByRoundedTime" should "split actual events at full hours" in {
    val correctMapped = correct2.map(x => CCCallReceived(1,2,x._1.getMillis,"hash", x._1.getMillis, x._2))
    EventTransformers.splitEventByRoundedTime(CCCallReceived(1,2,3,"hash", date.getMillis,7200000)) shouldBe correctMapped
  }
}

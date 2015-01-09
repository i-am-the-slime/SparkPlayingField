package org.menthal

import org.joda.time.DateTime
import org.menthal.aggregations.GeneralAggregations._
import org.menthal.aggregations.tools.EventTransformers
import org.menthal.model.{Granularity}
import org.menthal.model.events.{CCAggregationEntry, CCAppSession, MenthalEvent}
import org.menthal.model.implicits.DateImplicits._
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scalaz.Scalaz._
import scalaz._
class GeneralAggregationsSpec extends FlatSpec with GeneratorDrivenPropertyChecks with Matchers{
  val sc = SparkTestHelper.localSparkContext

  val appSession:Gen[CCAppSession] = for {
      userId ← Gen.choose(0L, 5L)
      timeOffsetInSeconds ← Gen.choose(0, 203902)
      time = DateTime.now().minusDays(100).plusSeconds(timeOffsetInSeconds).getMillis
      duration ← Gen.choose(0L, 100000L)
      packageName ← Gen.oneOf("com.less.offensive", "org.whatsapp", "org.menthal", "pl.konrad")
  } yield CCAppSession(userId, time, duration, packageName)

  val listAppSession:Gen[List[CCAppSession]] = Gen.listOf(appSession)

  def fromEventsToAggregations(aggregator: MenthalEventsAggregator)(sessions:List[MenthalEvent]):Iterator[CCAggregationEntry] = {
    val rdd = sc.parallelize(sessions)
    aggregator(rdd, Granularity.Hourly).toLocalIterator
  }

  def durations = fromEventsToAggregations(aggregateDuration) _
  def counts = fromEventsToAggregations(aggregateCount) _

  "counted aggregates" should "not be fewer after aggregation" in
    forAll (listAppSession) { sessions ⇒
      counts(sessions).foldLeft(0L)(_ + _.value) should be >= sessions.length.toLong
  }
  val timePeriod = Granularity.Hourly

  it should "be computed correctly" in {
    forAll (listAppSession) { sessions ⇒
      val keyVals = for {
        session ← sessions
        split ← EventTransformers.splitEventByRoundedTime(session, timePeriod)
        userId = session.userId
        pn = session.packageName
        bucket = Granularity.roundTimeFloor(split.time, timePeriod)
      } yield ((userId, pn, dateToLong(bucket)), 1)
      val expected = keyVals.groupBy(_._1).mapValues(_.length).toSet
      val result = counts(sessions).map(c ⇒ ((c.userId, c.key, c.time), c.value)).toSet
      result shouldBe expected
    }
  }

  "summed aggregates" should "have the same duration afterwards" in
    forAll (listAppSession) { sessions ⇒
      durations(sessions).foldLeft(0L)(_ + _.value) should be(sessions.foldMap(_.duration))
  }

  it should "be computed correctly" in {
    forAll (listAppSession) { sessions ⇒
      val keyVals = for {
        session ← sessions
        split ← EventTransformers.splitEventByRoundedTime(session, timePeriod)
        userId = session.userId
        pn = session.packageName
        bucket = Granularity.roundTimeFloor(split.time, timePeriod)
        duration = EventTransformers.getDuration(split)
      } yield ((userId, pn, dateToLong(bucket)), duration)
      val expected = keyVals.groupBy{ case (k,v) ⇒ k }.mapValues{_.foldMap(_._2)}.toSet
      val result = durations(sessions).map(c ⇒ ((c.userId, c.key, c.time), c.value)).toSet
      result shouldBe expected
    }
  }
}

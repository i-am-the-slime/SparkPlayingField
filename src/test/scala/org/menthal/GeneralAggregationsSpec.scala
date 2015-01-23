package org.menthal

import org.joda.time.DateTime
import org.menthal.aggregations.tools.{GeneralAggregators, EventTransformers}
import GeneralAggregators._
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



  def fromEventsToAggregations(aggregator: MenthalEventsAggregator)(sessions:List[MenthalEvent]):Iterator[CCAggregationEntry] = {
    val rdd = sc.parallelize(sessions)
    aggregator(rdd, Granularity.Hourly).toLocalIterator
  }

  def durations = fromEventsToAggregations(aggregateDuration) _
  def counts = fromEventsToAggregations(aggregateCount) _

  "counted aggregates" should "not be fewer after aggregation" in
    forAll (Generators.listAppSession) { sessions ⇒
      counts(sessions).foldLeft(0L)(_ + _.value) should be >= sessions.length.toLong
  }
  val timePeriod = Granularity.Hourly

  it should "be computed correctly" in {
    forAll (Generators.listAppSession) { sessions ⇒
      val keyVals = Generators.splitToBucketsWithCount(timePeriod, sessions)
      val expected = keyVals.groupBy(_._1).mapValues(_.length).toSet
      val result = counts(sessions).map(c ⇒ ((c.userId, c.key, c.time), c.value)).toSet
      result shouldBe expected
    }
  }

  "summed aggregates" should "have the same duration afterwards" in
    forAll (Generators.listAppSession) { sessions ⇒
      durations(sessions).foldLeft(0L)(_ + _.value) should be(sessions.foldMap(_.duration))
  }

  it should "be computed correctly" in {
    forAll (Generators.listAppSession) { sessions ⇒
      val keyVals = Generators.splitToBucketsWithDuration(timePeriod, sessions)
      val expected = keyVals.groupBy{ case (k,v) ⇒ k }.mapValues{_.foldMap(_._2)}.toSet
      val result = durations(sessions).map(c ⇒ ((c.userId, c.key, c.time), c.value)).toSet
      result shouldBe expected
    }
  }
}

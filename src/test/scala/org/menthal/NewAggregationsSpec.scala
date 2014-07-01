package org.menthal

import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

class NewAggregationsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

    "The function aggregate" should "take an RDD of String and return another RDD of String" in {
      val sc = SparkTestHelper.getLocalSparkContext
      val eventLines = Source.fromURL(getClass.getResource("/real_events.small")).getLines().toList
      val mockRDDs = sc.parallelize(eventLines)
      val events = NewAggregations.linesToEvents(mockRDDs)
      val result = NewAggregations.reduceToAppContainers(events)

      result.take(10).foreach{
        case (id, container) =>
          val sessions = container.sessions.filter(_.isInstanceOf[Session])
//          info(s"User $id\nSessions $sessions")
      }
      "beer" shouldBe "here"
      sc.stop()
    }
}

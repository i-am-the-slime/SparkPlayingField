package org.menthal

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers, FlatSpec}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.reflect.io.File
import scala.util.Try

class NewAggregationsSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  var sc:SparkContext = _

  override def beforeEach(){
    sc = SparkTestHelper.getLocalSparkContext
  }

  override def afterEach() = {
    sc.stop()
  }

//  "The function aggregate" should "take an RDD of String and return another RDD of String" in {
//    val eventLines = Source.fromURL(getClass.getResource("/real_events.small")).getLines().toList
//    val mockRDDs = sc.parallelize(eventLines)
//    val events = NewAggregations.linesToEvents(mockRDDs)
//    val result = NewAggregations.reduceToAppContainers(events)
//
//    result.take(10).foreach{
//      case (id, container) =>
//        val sessions = container.sessions.filter(_.isInstanceOf[Session])
////          info(s"User $id\nSessions $sessions")
//    }
//    "here" shouldBe "beer"
//  }
}

package org.menthal

import org.menthal.model.events.AppSession
import org.menthal.model.scalaevents.adapters.PostgresDump
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers, FlatSpec}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.reflect.io.File
import scala.util.Try

class AppSessionAggregationsSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  @transient var sc:SparkContext = _

  override def beforeEach(){
    sc = SparkTestHelper.localSparkContext
  }

  override def afterEach() = {
    sc.stop()
    sc = null
  }

  "The function aggregate" should "take an RDD of String and return another RDD of String" in {
    val eventLines = Source.fromURL(getClass.getResource("/real_events.small")).getLines().toList
    val mockRDDs = sc.parallelize(eventLines)
    val events = mockRDDs.flatMap(line => PostgresDump.tryToParseLineFromDump(line))
    val result = AppSessionAggregations.reduceToAppSessions(events)
    result.take(10).foreach{
      session:AppSession => info(s"User \nSessions $session")
    }
  }
}

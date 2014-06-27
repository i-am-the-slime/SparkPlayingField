package org.menthal

import org.menthal.model.events._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import org.apache.spark.{SparkConf, SparkContext}
import org.menthal.model.events.EventData._
import org.joda.time.DateTime

import scala.io.Source

class NewAggregationsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    def getLocalSparkContext: SparkContext = {
      val conf = new SparkConf()
        .setMaster("local")
        .setAppName("NewAggregationsSpec")
        .set("spark.executor.memory", "1g")
      val sc = new SparkContext(conf)
      sc
    }

    "The function aggregate" should "take an RDD of String and return another RDD of String" in {
      val sc = getLocalSparkContext
      val eventLines = Source.fromURL(getClass.getResource("/real_events.small")).getLines().toList.take(60)
      val mockRDDs = sc.parallelize(eventLines)
      val events = NewAggregations.linesToEvents(mockRDDs)
      val result = NewAggregations.reduceToAppContainers(events)
      result.take(1).foreach(
        x => info(x.toString())
      )
      "beer" shouldBe "here"
    }
}
  //Take all the file
  //filter
  //aggregate
  //write the result back

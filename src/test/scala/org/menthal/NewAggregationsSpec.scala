package org.menthal

import org.menthal.model.events._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import org.apache.spark.{SparkConf, SparkContext}
import org.menthal.model.events.EventData._
import org.joda.time.DateTime

/**
 * Created by mark on 04.06.14.
 */
class NewAggregationsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    def getLocalSparkContext: SparkContext = {
      val conf = new SparkConf()
        .setMaster("local")
        .setAppName("NewAggregationsSpec")
        .set("spark.executor.memory", "1g")
      val sc = new SparkContext(conf)
      sc
    }

    def e[A <: EventData](user:Int, t:DateTime, data:A):Event= Event(0, user, data, t)
    val now = DateTime.now()
    val wsc1 = WindowStateChanged("", "com.angrybirds", "")
    val wsc2 = WindowStateChanged("", "com.whatsapp", "")
    val lock = ScreenOff()
    val unlock = ScreenUnlock()
    val fabricatedEvents:List[Event] = List(
      e(1, now plusMinutes 0, wsc1),
      e(1, now plusMinutes 7, unlock),
      e(1, now plusMinutes 1, wsc2),
      e(1, now plusMinutes 2, lock),
      e(1, now plusMinutes 3, unlock),
      e(1, now plusMinutes 4, wsc1),
      e(1, now plusMinutes 5, wsc2),
      e(1, now plusMinutes 6, lock),
      e(1, now plusMinutes 8, wsc2),
      e(1, now plusMinutes 9, wsc1)
    )

//    "aggregate" should "take an RDD of String and return another RDD of String" in {
//      val sc = getLocalSparkContext
//      val eventLines = Source.fromURL(getClass.getResource("/real_events.small")).getLines().toList.take(60)
////      val eventLines = fabricatedEvents
//      val mockRDDs = sc.parallelize(eventLines)
//      val events = NewAggregations.linesToEvents(mockRDDs)
//      val result = NewAggregations.reduceToAppContainers(events)
//      result.take(20).foreach(
//        x => info(x.toString())
//      )
//    }
}
  //Take all the file
  //filter
  //aggregate
  //write the result back

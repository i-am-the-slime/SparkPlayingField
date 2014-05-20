package org.menthal

import Aggregations._
import org.joda.time.DateTime
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec

/**
 * Created by mark on 19.05.14.
 */

class AggregationsSpec extends FlatSpec {
  def getLocalSparkContext:SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("AggregationsSpec")
      .set("spark.executor.memory", "1g")

     new SparkContext(conf)
  }

  "An example dump" should "be parsed correctly" in {
    val sc = getLocalSparkContext
    val mockData = ("251589\t154\t2013-07-22 13:43:29.332+02\t1007\t\"[\\\\\"gps\\\\\",\\\\\"29.0\\\\\",\\\\\"7.12153107\\\\\",\\\\\"50.73606839\\\\\"]\"" +
      "\n251590\t154\t2013-07-22 13:33:05.36+02\t64\t\"[\\\\\"Hangouts\\\\\",\\\\\"com.google.android.talk\\\\\",20]\"" +
      "\n251591\t2\t2013-07-22 15:41:19+02\t3000\t{\"start\":1374500463000,\"app\":\"com.menthal.nyx\",\"end\":1374500479000}" +
      "\n251592\t2\t2013-07-22 15:41:19+02\t3001\t{\"points\":5}" +
      "\n251593\t2\t2013-07-22 15:41:19+02\t3000\t{\"start\":1374500479000,\"app\":\"android\",\"end\":1374500479000}" +
      "\n251594\t2\t2013-07-22 15:56:20+02\t3001\t{\"points\":8}" +
      "\n251595\t2\t2013-07-22 16:01:53+02\t3001\t{\"points\":2}" +
      "\n251596\t2\t2013-07-22 16:01:54+02\t3000\t{\"start\":1374501712000,\"app\":\"com.menthal.nyx\",\"end\":1374501714000}" +
      "\n251597\t2\t2013-07-22 16:01:56+02\t3000\t{\"start\":1374501714000,\"app\":\"com.menthal.nyx\",\"end\":1374501716000}" +
      "\n251598\t2\t2013-07-22 16:02:57+02\t3000\t{\"start\":1374501716000,\"app\":\"com.menthal.nyx\",\"end\":1374501777000}" +
      "\n251599\t2\t2013-07-22 16:02:57+02\t3001\t{\"points\":3}")
      .split("\n")
    val mockRDDs = sc.parallelize(mockData)
    val aggr = aggregate(mockRDDs)
    val collected = aggr.collect()

    val time1 = DateTime.parse("2013-07-22T15:00:00+02")
    val time2 = DateTime.parse("2013-07-22T16:00:00+02")
    collected.foreach{
      case ((long, datetime), map) =>
        if(datetime == time1)
          assert(map.getOrElse("points",0) == 13)
        else if(datetime == time2)
          assert(map.getOrElse("points",0) == 5)
        else
          fail(s"Hours do not get parsed correctly. Expected the date to be either $time1 or $time2")
    }

    assert(collected.length > 0)
    sc.stop()
  }

  "A valid split line " should "be converted to the appropriate Event" in {
    val validLine = "111988\t7\t2013-03-22 12:30:36+01\t32\t[\"Winamp\",\"com.nullsoft.winamp/com.nullsoft.winamp.MusicBrowserActivity\",\"[Winamp]\"]"
    val event = cookEvent(validLine.split("\t"))
    assert(event.isDefined)
    assert(event.get.data.eventType == Event.TYPE_WINDOW_STATE_CHANGED)
  }

  "A malformed split line" should "result in a None" in {
    val invalidLine = "hey you asshole"
    val event = cookEvent(invalidLine.split("\t"))
    assert(event.isEmpty)
  }

  "Time" should "start at 0 minutes 0 seconds and 0 milliseconds after rounding" in {
    val exampleDateTime = DateTime.parse("2013-03-22T12:30:36+01")
    val zeroedDateTime = roundTime(exampleDateTime)
    assert(zeroedDateTime.getMinuteOfHour == 0)
    assert(zeroedDateTime.getSecondOfMinute == 0)
    assert(zeroedDateTime.getMillisOfSecond == 0)
  }

}

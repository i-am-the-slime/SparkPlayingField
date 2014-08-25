package org.menthal

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.menthal.model.events.{CCCallReceived, CCSmsReceived, MenthalEvent}

import org.menthal.model.events.MenthalUtils._
import org.menthal.model.implicits.EventImplicts._
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import com.twitter.algebird.Semigroup
import org.menthal.model.scalaevents.adapters.PostgresDump
import com.twitter.algebird.Operators._

/**
 * Created by mark on 18.05.14.
 */
object GeneralAggregations {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: Aggregations <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Aggregations", System.getenv("SPARK_HOME"))
    val dumpFile = "/data"
    val eventsDump = sc.textFile(dumpFile, 2)
    aggregateHourlyFromString(eventsDump, receivedSmsFilter)
    sc.stop()
  }

  type UserBucketsRDD[A] = RDD[(((Long, DateTime), A))]
  type EventPredicate[A] = MenthalEvent => Boolean


  def getEventsFromLines(lines: RDD[String], filter: MenthalEvent => Boolean): RDD[MenthalEvent] = {
    for {
      line <- lines
      event <- PostgresDump.tryToParseLineFromDump(line)
      if filter(event)
    } yield event
  }

  def receivedSmsFilter(event: MenthalEvent): Boolean =
    event.isInstanceOf[CCSmsReceived]


  def aggregateHourlyFromString(lines: RDD[String], filter: MenthalEvent => Boolean): RDD[(((Long, DateTime), Map[String, Long]))] = {
    val events = getEventsFromLines(lines, filter)
    aggregateHourlyMap(events)
  }

  def aggregateHourlyMap(events: RDD[MenthalEvent]): UserBucketsRDD[Map[String, Long]] = {
    val buckets: UserBucketsRDD[Map[String, Long]] = events.map {
      e => ((e.userId, roundTime(new DateTime(e.time))), eventAsMap(e))
    }
    buckets reduceByKey (_ + _)
  }

  def aggregateHourlyCounter(events: RDD[MenthalEvent]): UserBucketsRDD[Map[String, Int]] = {
    val buckets = events.map {
        e => ((e.userId, roundTime(new DateTime(e.time))), eventAsCounter(e))
    }
    buckets reduceByKey (_ + _)
  }
}


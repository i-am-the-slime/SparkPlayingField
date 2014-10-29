package org.menthal

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.menthal.model.events.Granularity._
import org.menthal.model.events.{CCAggregation, CCCallReceived, CCSmsReceived, MenthalEvent}

import org.menthal.model.events.MenthalUtils._
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
    aggregateFromString(eventsDump, receivedSmsFilter)
    sc.stop()
  }

  type UserBucketsRDD[A] = RDD[(((Long, DateTime), A))]
  type EventPredicate = MenthalEvent => Boolean


  def getEventsFromLines(lines: RDD[String], filter: EventPredicate): RDD[MenthalEvent] = {
    for {
      line <- lines
      event <- PostgresDump.tryToParseLineFromDump(line)
      if filter(event)
    } yield event
  }

  def receivedSmsFilter(event: MenthalEvent): Boolean =
    event.isInstanceOf[CCSmsReceived]

  def aggregateFromString(lines: RDD[String], filter: EventPredicate): UserBucketsRDD[Map[String, Long]] = {
    val events = getEventsFromLines(lines, filter)
    reduceToUserBucketsMap(events, Hourly)
  }

  def aggregateEvents(events: RDD[MenthalEvent], granularity: Granularity, name: String): RDD[CCAggregation] = {
    val buckets = reduceToUserBucketsMap(events, granularity)
    buckets.map {case ((user, time), bucket) => CCAggregation(user, time, 1, name, bucket.toString()) } //TODO: fix
  }

  def reduceToUserBucketsMap(events: RDD[MenthalEvent], granularity: Granularity): UserBucketsRDD[Map[String, Long]] = {
    val buckets = events.map {
      e => ((e.userId, roundTime(e.time, granularity)), eventAsMap(e))
    }
    buckets reduceByKey (_ + _)
  }

  def reduceToUserBucketCounter(events: RDD[MenthalEvent], granularity: Granularity): UserBucketsRDD[Map[String, Int]] = {
    val buckets = events.map {
        e => ((e.userId, roundTime(e.time, granularity)), eventAsCounter(e))
    }
    buckets reduceByKey (_ + _)
  }
}


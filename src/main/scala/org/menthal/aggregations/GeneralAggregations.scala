package org.menthal.aggregations

import com.twitter.algebird.Operators._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.menthal.io.postgres.PostgresDump
import org.menthal.model.Granularity
import org.menthal.model.Granularity.Granularity
import org.menthal.model.implicits.DateImplicits.{dateToLong,longToDate}
import org.menthal.aggregations.tools.EventTransformers._
import org.menthal.model.events.{CCAggregation, CCSmsReceived, MenthalEvent}
import scala.collection.mutable.{Map => MMap}

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
  type UserAggregatesRDD = UserBucketsRDD[Map[String, Long]]
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

  def aggregateFromString(lines: RDD[String], filter: EventPredicate): UserAggregatesRDD = {
    val events = getEventsFromLines(lines, filter)
    reduceToUserBucketsMap(events, Granularity.Hourly)
  }

  def aggregateEvents(events: RDD[MenthalEvent], granularity: Granularity, name: String): RDD[CCAggregation] = {
    val buckets = reduceToUserBucketsMap(events, granularity)
    buckets.map {case ((user, time), bucket) =>
      CCAggregation(user, dateToLong(time), 1L, name, MMap(bucket.toSeq: _*))}
  }

  def reduceToUserBucketsMap(events: RDD[MenthalEvent], granularity: Granularity): UserAggregatesRDD = {
    val buckets = events.map {
      e => ((e.userId, Granularity.roundTime(longToDate(e.time), granularity)), eventAsMap(e))
    }
    buckets reduceByKey (_ + _)
  }

  def reduceToUserBucketCounter(events: RDD[MenthalEvent], granularity: Granularity): UserAggregatesRDD = {
    val buckets = events.map {
        e => ((e.userId, Granularity.roundTime(longToDate(e.time), granularity)), eventAsCounter(e))
    }
    buckets reduceByKey (_ + _)
  }
}


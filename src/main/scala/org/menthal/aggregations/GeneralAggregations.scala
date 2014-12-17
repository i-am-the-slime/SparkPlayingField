package org.menthal.aggregations

import com.twitter.algebird.Operators._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.menthal.io.parquet.ParquetIO
import org.menthal.io.postgres.PostgresDump
import org.menthal.model.{EventType, Granularity}
import org.menthal.model.Granularity.Granularity
import Granularity._
import org.menthal.model.implicits.DateImplicits.{dateToLong,longToDate}
import org.menthal.aggregations.tools.EventTransformers._
import org.menthal.model.events.{AggregationEntry, CCAggregationEntry, CCSmsReceived, MenthalEvent}
import scala.collection.mutable
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

  type PerUserBucketsRDD[K, V] = RDD[(((Long, DateTime, K), V))]
  type PerUserDurationAggregatesRDD = PerUserBucketsRDD[String, Long]
  type PerUserCountAggregatesRDD = PerUserBucketsRDD[String, Long]
  type UserMapBucketsRDD[K, V] = RDD[(((Long, DateTime),Map[K,V]))]
  type UserDurationMapAggregatesRDD = UserMapBucketsRDD[String, Long]
  type UserCountMapAggregatesRDD = UserMapBucketsRDD[String, Long]
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

  def aggregateFromString(lines: RDD[String], filter: EventPredicate): UserDurationMapAggregatesRDD = {
    val events = getEventsFromLines(lines, filter)
    reduceToUserBucketsMap(events, Granularity.Hourly)
  }


  def aggregateFromParquet[A <: MenthalEvent](datadir: String, eventType: Int, granularity: Granularity, outputName: String, sc: SparkContext) = {
    val events = ParquetIO.readEventType[A](datadir, eventType, sc)
    val aggregates = aggregateEvents(events, granularity)
    ParquetIO.write(sc, aggregates, datadir + "/" + outputName, AggregationEntry.getClassSchema)
  }


  def reduceToPerUserAggregations[K<:Numeric](getValFunction: MenthalEvent => K)
                                             (events: RDD[_ <: MenthalEvent],
                                              granularity: Granularity)
                                             :PerUserBucketsRDD[String, K] ={
    val buckets = for {
      event <- events
      e <- splitEventByRoundedTime(event, granularity)
      id = e.userId
      timeBucket = roundTimeFloor(longToDate(e.time), granularity)
      key = getKeyFromEvent(e)
    } yield ((id, timeBucket, key), getValFunction(e))
    buckets reduceByKey (_ + _)
  }

  def reduceToPerUserDurationAggregations = reduceToPerUserAggregations(getDuration)
  def reduceToPerUserCountAggregations = reduceToPerUserAggregations[Long](_ => 1L)


  def aggregateEvents(events: RDD[_ <: MenthalEvent], granularity: Granularity): RDD[CCAggregationEntry] = {
    val buckets = reduceToPerUserDurationAggregations(events, granularity)
    buckets.map {case ((user, time, key), value) =>
      CCAggregationEntry(user, dateToLong(time), granularityToLong(granularity), key, value)}
  }

  def reduceToUserBucketsMap(events: RDD[MenthalEvent], granularity: Granularity): UserDurationMapAggregatesRDD = {
    val buckets = events.map {
      e => ((e.userId, roundTimeFloor(longToDate(e.time), granularity)),  eventAsMap(e))
    }
    buckets reduceByKey (_ + _)
  }

  def reduceToUserBucketCounter(events: RDD[MenthalEvent], granularity: Granularity): UserCountMapAggregatesRDD = {
    val buckets = events.map {
        e => ((e.userId, roundTimeFloor(longToDate(e.time), granularity)), eventAsCounter(e))
    }
    buckets reduceByKey (_ + _)
  }

  def aggregateEventsThroughMap(events: RDD[MenthalEvent], granularity: Granularity, name: String): RDD[CCAggregationEntry] = {
    val buckets = reduceToUserBucketsMap(events, granularity)
    buckets.flatMap {case ((user, time), bucket) =>
      for ((k,v) <- bucket)
      yield  CCAggregationEntry(user, dateToLong(time), 1L, k, v)
    }
  }
}


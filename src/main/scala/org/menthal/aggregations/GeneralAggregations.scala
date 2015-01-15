package org.menthal.aggregations

import com.twitter.algebird.Operators._
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.menthal.aggregations.tools.EventTransformers._
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.Granularity
import org.menthal.model.Granularity.TimePeriod
import org.menthal.model.events.Implicits._
import org.menthal.model.events.{AggregationEntry, CCAggregationEntry, MenthalEvent}
import org.menthal.model.implicits.DateImplicits.{dateToLong, longToDate}

import scala.collection.mutable.{Map => MMap}
import scala.reflect.ClassTag
import org.menthal.model.EventType._

/**
 * Created by mark on 18.05.14.
 */
object GeneralAggregations {

  type PerUserBucketsRDD[K, V] = RDD[(((Long, DateTime, K), V))]
  type MenthalEventsAggregator = (RDD[MenthalEvent], TimePeriod) => RDD[CCAggregationEntry]


  def aggregateLength:MenthalEventsAggregator = aggregateEvents(getMessageLength) _
  def aggregateDuration:MenthalEventsAggregator = aggregateEvents(getDuration) _
  def aggregateCount:MenthalEventsAggregator = aggregateEvents(_ => 1L) _


  def aggregateAggregations(aggrs: RDD[MenthalEvent], granularity: TimePeriod, subgranularity: TimePeriod): RDD[CCAggregationEntry] = {
      val buckets = for {
        CCAggregationEntry(user, time, `subgranularity`, key, value) ← aggrs
        timeBucket = Granularity.roundTimeFloor(time, granularity)
      } yield ((user, timeBucket, key), value)
      buckets reduceByKey (_ + _) map { case ((user, time, key), value) =>
        CCAggregationEntry(user, time, granularity.toInt, key, value)
      }
  }

  def aggregateEvents(fn:MenthalEvent ⇒ Long)
                     (events: RDD[MenthalEvent], granularity: TimePeriod)
                     :RDD[CCAggregationEntry] = {
    val buckets = reduceToPerUserAggregations(fn)(events, granularity)
    buckets.map {case ((user, time, key), value) ⇒
      CCAggregationEntry(user, time, granularity.toInt, key, value)}
  }

  def reduceToPerUserAggregations(getValFunction: MenthalEvent => Long)
                                 (events: RDD[MenthalEvent], granularity: TimePeriod)
                                 :PerUserBucketsRDD[String, Long] = {
    val buckets = for {
      event ← events
      e ← splitEventByRoundedTime(event, granularity)
      id = e.userId
      timeBucket = Granularity.roundTimeFloor(e.time, granularity)
      key = getKeyFromEvent(e)
    } yield ((id, timeBucket, key), getValFunction(e))
    buckets reduceByKey (_ + _)
  }

}


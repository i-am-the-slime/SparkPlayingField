package org.menthal.aggregations.tools

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.joda.time.DateTime
import org.menthal.aggregations.tools.EventTransformers._
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.Granularity
import org.menthal.model.Granularity.{Timestamp, TimePeriod, roundTimestamp, roundTimeFloor}
import org.menthal.model.events.{CCAggregationEntry, MenthalEvent}
import org.menthal.model.implicits.DateImplicits.{dateToLong, longToDate}
import org.apache.spark.sql.functions._

import scala.collection.mutable.{Map => MMap}

/**
 * Created by mark on 18.05.14.
 */
object GeneralAggregators {

  type PerUserBucketsRDD[K, V] = RDD[(((Long, Timestamp, K), V))]
  type MenthalEventsAggregator = (RDD[MenthalEvent], TimePeriod) => RDD[CCAggregationEntry]
  type MenthalDatasetAggregator[T<:MenthalEvent] = (Dataset[T], TimePeriod) => Dataset[CCAggregationEntry]


  def aggregateLength: MenthalEventsAggregator = aggregateEvents(getMessageLength) _

  def aggregateDuration: MenthalEventsAggregator = aggregateEvents(getDuration) _

  def aggregateCount: MenthalEventsAggregator = aggregateEvents(_ => 1L) _


  private def aggregateAggregations(aggrs: RDD[MenthalEvent], granularity: TimePeriod, subgranularity: TimePeriod): RDD[CCAggregationEntry] = {
    val buckets = for {
      CCAggregationEntry(user, time, `subgranularity`, key, value) ← aggrs
      timeBucket = roundTimeFloor(time, granularity)
    } yield ((user, timeBucket, key), value)
    buckets reduceByKey (_ + _) map { case ((user, time, key), value) =>
      CCAggregationEntry(user, time, granularity.toInt, key, value)
    }
  }

  private def aggregateEvents(fn: MenthalEvent ⇒ Long)
                     (events: RDD[MenthalEvent], granularity: TimePeriod)
  : RDD[CCAggregationEntry] = {
    val buckets = reduceToPerUserAggregations(fn)(events, granularity)
    buckets.map { case ((user, time, key), value) ⇒
      CCAggregationEntry(user, time, granularity.toInt, key, value)
    }
  }


  private def reduceToPerUserAggregations(getValFunction: MenthalEvent => Long)
                                 (events: RDD[MenthalEvent], granularity: TimePeriod)
  : PerUserBucketsRDD[String, Long] = {
    //val distinctEvents = events.map(e => (e.time, e.userId, getKeyFromEvent(e), getValFunction(e)))
    val tuples = for {
      event ← events
      e ← splitEventByRoundedTime(event, granularity)
      id = e.userId
      time = e.time
      k = getKeyFromEvent(e)
      v = getValFunction(e)
    } yield (id, time, k, v)
    val buckets = for {
      (id, time, k, v) <- tuples//.distinct
      timeBucket = roundTimestamp(time, granularity)
    } yield ((id, timeBucket, k), v)
    buckets reduceByKey (_ + _)
  }

  private def aggregateAggregationsToParquet(aggrName:String, subAggregates: RDD[MenthalEvent],
                                     granularity: TimePeriod, subgranularity: TimePeriod)
                                    (implicit sc: SparkContext, datadir: String)
                                    :RDD[_ <: MenthalEvent] = {
    val aggregates = GeneralAggregators.aggregateAggregations(subAggregates, granularity, subgranularity)
    ParquetIO.writeAggrType(sc, datadir, aggrName, granularity, aggregates.map(_.toAvro), overwrite = true)
    aggregates
  }
  private def aggregateEventsToParquet(aggrName: String, aggregator:MenthalEventsAggregator,
                               eventType: Int, events: RDD[MenthalEvent],granularity: TimePeriod)
                              (implicit sc: SparkContext, datadir: String)
                              :RDD[CCAggregationEntry]= {
    val aggregates = aggregator(events, granularity)
    ParquetIO.writeAggrType(sc, datadir, aggrName, granularity, aggregates.map(_.toAvro), overwrite = true)
    aggregates
  }
  def aggregateToParquetForGranularity(aggrName: String, aggregator:MenthalEventsAggregator, eventType: Int)
                                      (events: RDD[MenthalEvent],granularity: TimePeriod)
                                      (implicit sc: SparkContext, datadir: String)
                                      :RDD[MenthalEvent] = {
    Granularity.sub(granularity) match {
      case Some(subgranularity) ⇒ aggregateAggregationsToParquet(aggrName, events, granularity, subgranularity).map(_.asInstanceOf[MenthalEvent])
      case None ⇒ aggregateEventsToParquet(aggrName, aggregator, eventType, events, granularity).map(_.asInstanceOf[MenthalEvent])
    }
  }


  private def splitAndGroupByUserTimePeriodKey[T<: MenthalEvent](events: Dataset[T], granularity: TimePeriod)
                                           (implicit sqlContext: SQLContext)
                        :GroupedDataset[(Long, Long, String), SimpleMenthalEvent] = {
    import sqlContext.implicits._
    val splittedEvents:Dataset[SimpleMenthalEvent] = events.flatMap(splitEventToTuplesByRoundedTime(_, granularity))
    splittedEvents.groupBy(e => (e.userId, roundTimestamp(e.time, granularity), e.key))
  }
  private def splitAndGroupByUserTimePeriod[T<: MenthalEvent](events: Dataset[T], granularity: TimePeriod)
                                                                (implicit sqlContext: SQLContext)
  :GroupedDataset[(Long, Long, String), SimpleMenthalEvent] = {
    import sqlContext.implicits._
    val splittedEvents:Dataset[SimpleMenthalEvent] = events.flatMap(splitEventToTuplesByRoundedTime(_, granularity))
    splittedEvents.groupBy(e => (e.userId, roundTimestamp(e.time, granularity), "key"))
  }

  private def toAggregations(aggregatedEvents: Dataset[((Long, Long, String), Long)], granularity:TimePeriod)
                    (implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    aggregatedEvents.map {
      case ((user, time, key), value) => CCAggregationEntry(user, time, granularity.toInt, key, value)
    }
  }

  def countEvents[T<: MenthalEvent](events: Dataset[T], granularity: TimePeriod)
                                   (implicit sqlContext: SQLContext):Dataset[CCAggregationEntry] = {
    splitAndGroupByUserTimePeriodKey(events, granularity)
                      .count()
                      .transform(toAggregations(_, granularity))
  }

  def sumEvents[T<: MenthalEvent](events: Dataset[T], granularity: TimePeriod)
                                 (implicit sqlContext: SQLContext):Dataset[CCAggregationEntry]= {
    import sqlContext.implicits._
    splitAndGroupByUserTimePeriodKey(events, granularity)
                        .agg(sum("value").as[Long])
                        .transform(toAggregations(_, granularity))
  }

  def countKeys[T<: MenthalEvent](events: Dataset[T], granularity: TimePeriod)
                                 (implicit sqlContext: SQLContext):Dataset[CCAggregationEntry]= {
    import sqlContext.implicits._
    splitAndGroupByUserTimePeriod(events, granularity)
      .agg(countDistinct("key").as[Long])
      .transform(toAggregations(_, granularity))
  }

  private def aggregateAggregations(aggregations: Dataset[CCAggregationEntry], granularity: TimePeriod)
                           (implicit sqlContext: SQLContext): Dataset[CCAggregationEntry] = {
      import sqlContext.implicits._
      aggregations.groupBy(a => (a.userId, roundTimestamp(a.time, granularity), a.key))
                 .agg(sum("value").as[Long])
                 .transform(toAggregations(_, granularity))
  }


  private def aggregateAggregationsToParquet(aggrName:String, subAggregates: Dataset[CCAggregationEntry],
                                     granularity: TimePeriod)
                                    (implicit sqlContext: SQLContext, datadir: String)
  :Dataset[CCAggregationEntry] = {
    val aggregates = aggregateAggregations(subAggregates, granularity)
    ParquetIO.writeAggrTypeDataset(datadir, aggrName, granularity, aggregates, overwrite = true)
    aggregates
  }

  private def aggregateEventsToParquet[T<: MenthalEvent](aggrName: String, aggregator:MenthalDatasetAggregator[T],
                               events: Dataset[T] ,granularity: TimePeriod, cached:Boolean = false)
                              (implicit sqlContext: SQLContext, datadir: String)
  :Dataset[CCAggregationEntry]= {
    val aggregates = aggregator(events, granularity)
    if (cached) aggregates.cache()
    ParquetIO.writeAggrTypeDataset(datadir, aggrName, granularity, aggregates, overwrite = true)
    aggregates
  }

  def aggregateToParquet[T <: MenthalEvent](aggrName: String, aggregator:MenthalDatasetAggregator[T],
                                       events: Dataset[T], additive:Boolean = true)
                                      (implicit sqlContext: SQLContext, datadir: String)
                                       :Unit = {
//    for (granularity <- List(//Granularity.FiveMin, Granularity.FifteenMin,
//                           Granularity.Hourly))
//        aggregateEventsToParquet(aggrName, aggregator, events, granularity)
    if (additive) {
      val dailyAggregates = aggregateEventsToParquet(aggrName, aggregator, events, Granularity.Daily, cached = true)
      for (granularity <- List(Granularity.Weekly, Granularity.Monthly, Granularity.Yearly))
        aggregateAggregationsToParquet(aggrName, dailyAggregates, granularity)
      dailyAggregates.unpersist()
    } else {
      for (granularity <- List(Granularity.Daily, Granularity.Weekly, Granularity.Monthly, Granularity.Yearly))
        aggregateEventsToParquet(aggrName, aggregator, events, granularity)
    }
  }


}


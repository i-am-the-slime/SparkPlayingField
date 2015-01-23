package org.menthal.aggregations.tools

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import GeneralAggregators.MenthalEventsAggregator
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.Granularity
import org.menthal.model.Granularity.{GranularityForest, TimePeriod}
import org.menthal.model.events.{CCAggregationEntry, MenthalEvent}

/**
 * Created by mark on 09.01.15.
 */
case class AggrSpec[A <: SpecificRecord](eventType: Int,
                                         converter: A ⇒ MenthalEvent,
                                         aggregators: List[(MenthalEventsAggregator, String)])

object AggrSpec {
  def apply[A <: SpecificRecord](eventType: Int, converter: A ⇒ MenthalEvent, aggs: (MenthalEventsAggregator, String)*): AggrSpec[A] = {
    AggrSpec(eventType, converter, aggregators = aggs.toList)
  }

  def count(name: String) = ( GeneralAggregators.aggregateCount, name)

  def duration(name: String) = (GeneralAggregators.aggregateDuration, name)

  def length(name: String) = (GeneralAggregators.aggregateLength, name)

  def countAndDuration(nameCount: String, nameDuration: String) = List(count(nameCount), duration(nameDuration))

  def countAndLength(nameCount: String, nameLength: String) = List(count(nameCount), length(nameLength))

  def aggregate(sc: SparkContext, datadir: String, suite: List[AggrSpec[_ <: SpecificRecord]], granularities: GranularityForest) = {
    def aggregateAggregationsToParquet(aggrName:String, subAggregates: RDD[MenthalEvent], granularity: TimePeriod, subgranularity: TimePeriod)
        :RDD[_ <: MenthalEvent] = {
      val aggregates = GeneralAggregators.aggregateAggregations(subAggregates, granularity, subgranularity)
      ParquetIO.writeAggrType(sc, datadir, aggrName, granularity, aggregates.map(_.toAvro))
      aggregates
    }
    def aggregateEventsToParquet
        (aggrName: String, aggregator:MenthalEventsAggregator, eventType: Int,events: RDD[MenthalEvent],granularity: TimePeriod)
        :RDD[CCAggregationEntry]= {
      val aggregates = aggregator(events, granularity)
      ParquetIO.writeAggrType(sc, datadir, aggrName, granularity, aggregates.map(_.toAvro))
      aggregates
    }
    def aggregateToParquetForGranularity
        (aggrName: String, aggregator:MenthalEventsAggregator, eventType: Int)
        (events: RDD[MenthalEvent],granularity: TimePeriod)
        :RDD[MenthalEvent] = {
      Granularity.sub(granularity) match {
        case Some(subgranularity) ⇒ aggregateAggregationsToParquet(aggrName, events, granularity, subgranularity).map(_.asInstanceOf[MenthalEvent])
        case None ⇒ aggregateEventsToParquet(aggrName, aggregator, eventType, events, granularity).map(_.asInstanceOf[MenthalEvent])
      }
    }
    for {
      AggrSpec(eventType, converter, aggrs) ← suite
      events = ParquetIO.readEventType(sc, datadir, eventType).map(converter)
      (aggregator, aggrName) ← aggrs
      granularityTree ← granularities
      parquetAggregator = aggregateToParquetForGranularity(aggrName, aggregator, eventType) _
    }   granularityTree.traverseTree(events)(parquetAggregator)

  }

}







package org.menthal.aggregations

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.menthal.aggregations.GeneralAggregations.MenthalEventsAggregator
import org.menthal.aggregations.tools.{Tree,Leaf, Node}
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.EventType._
import org.menthal.model.Granularity
import org.menthal.model.Granularity.TimePeriod
import org.menthal.model.events.Implicits._
import org.menthal.model.events.{AppSession, CCAggregationEntry, AggregationEntry, MenthalEvent}

import scala.reflect.ClassTag

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

  def agCount(name: String, suffix: String = "count") = ( GeneralAggregations.aggregateCount, name + "_" + suffix)

  def agDuration(name: String, suffix: String = "duration") = (GeneralAggregations.aggregateDuration, name + "_" + suffix)

  def agLength(name: String, suffix: String = "length") = (GeneralAggregations.aggregateLength, name + "_" + suffix)

  def agCountAndDuration(name: String) = List(agCount(name), agDuration(name))

  def agCountAndLength(name: String) = List(agCount(name), agLength(name))


  def aggregate(sc: SparkContext, datadir: String, suite: List[AggrSpec[_ <: SpecificRecord]], granularities: List[Tree[TimePeriod]]) = {
    def aggregateAggregationsToParquet(aggrName:String, subAggregates: RDD[MenthalEvent], granularity: TimePeriod, subgranularity: TimePeriod)
        :RDD[_ <: MenthalEvent] = {
      val aggregates = GeneralAggregations.aggregateAggregations(subAggregates, granularity, subgranularity)
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
//    for {
//      AggrSpec(eventType, converter, aggrs) ← suite
//      (aggregator, aggrName) ← aggrs
//      granularityTree ← granularities
//    }  {
//val events = ParquetIO.readEventType(sc, datadir, eventType).map(converter)
//  .traverseTree(events)(parquetAggregator) }

    val granularities = Leaf(Granularity.Hourly)
    val simpleAggrSpecs = List(
      AggrSpec(TYPE_APP_SESSION, toCCAppSession _, agCount("app", "starts")))
    val parquetAggregator = aggregateToParquetForGranularity("app_starts", GeneralAggregations.aggregateCount, TYPE_APP_SESSION) _
    val events:RDD[MenthalEvent] = ParquetIO.readEventType[AppSession](sc, datadir, TYPE_APP_SESSION).map(toCCAppSession)
    granularities.traverseTree(events)(parquetAggregator)


  }

}





package org.menthal.aggregations

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.menthal.aggregations.AggrSpec.ParquetEventsAggregator
import org.menthal.aggregations.GeneralAggregations.MenthalEventsAggregator
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.EventType._
import org.menthal.model.Granularity
import org.menthal.model.Granularity.TimePeriod
import org.menthal.model.events.Implicits._
import org.menthal.model.events.{AggregationEntry, MenthalEvent}

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

  val suite = List(
    AggrSpec(TYPE_APP_SESSION, toCCAppSession, agDuration("app", "usage"), agCount("app", "starts")),
    AggrSpec(TYPE_CALL_MISSED, toCCCallMissed _, agCount("call_missed")),
    AggrSpec(TYPE_CALL_OUTGOING, toCCCallOutgoing _, agCountAndDuration("call_outgoing")),
    AggrSpec(TYPE_CALL_RECEIVED, toCCCallReceived _, agCountAndDuration("call_received")),
    AggrSpec(TYPE_NOTIFICATION_STATE_CHANGED, toCCNotificationStateChanged _, agCount("notification")),
    AggrSpec(TYPE_SCREEN_OFF, toCCScreenOff _, agCount("screen_off")),
    AggrSpec(TYPE_SCREEN_ON, toCCScreenOn _, agCount("screen_on")),
    AggrSpec(TYPE_SCREEN_UNLOCK, toCCScreenUnlock _, agCount("screen_unlock")),
    AggrSpec(TYPE_SMS_RECEIVED, toCCSmsReceived _, agCountAndLength("message_received")),
    AggrSpec(TYPE_SMS_SENT, toCCSmsSent _, agCountAndLength("message_sent")),
    AggrSpec(TYPE_WHATSAPP_RECEIVED, toCCWhatsAppReceived _, agCountAndLength("whatsapp_received")),
    AggrSpec(TYPE_WHATSAPP_SENT, toCCWhatsAppSent _, agCountAndLength("whatsapp_sent"))
  )


  def aggregateSuiteForGranularity(granularity:TimePeriod, datadir: String, sc: SparkContext) = {
    for {
      AggrSpec(eventType, converter, aggrs) ← suite
      (aggregator, aggrName) ← aggrs
    } {
      Granularity.sub(granularity) match {
        case Some(subgranularity) ⇒
          aggregateAggregationsFromParquet(datadir, aggrName, granularity, subgranularity, sc)
        case None ⇒
          aggregateEventsFromParquet(aggregator)(converter)(datadir, eventType, granularity, aggrName, sc)
      }
    }
  }

  def aggregateAggregationsFromParquet(datadir: String, aggrName: String, granularity: TimePeriod, subgranularity:TimePeriod, sc: SparkContext) = {
    val subAggregates = ParquetIO.read[AggregationEntry](datadir + "/" + aggrName + Granularity.asString(subgranularity),sc).map(toCCAggregationEntry)
    val aggregates = GeneralAggregations.aggregateAggregations(subAggregates, granularity, subgranularity)
    ParquetIO.write(sc, aggregates.map(_.toAvro), datadir + "/" + aggrName + Granularity.asString(granularity), AggregationEntry.getClassSchema)
  }

  type EventConverter[A] = A ⇒ MenthalEvent
  type ParquetEventsAggregator[A] = (EventConverter[A]) ⇒ (String, Int, TimePeriod, String, SparkContext) ⇒ Unit
  type ParquetAggregationsAggregator = (String, String, TimePeriod, TimePeriod, SparkContext) ⇒ Unit

  def aggregateDurationFromParquet:ParquetEventsAggregator =
    aggregateEventsFromParquet(GeneralAggregations.aggregateDuration) _
  def aggregateCountFromParquet:ParquetEventsAggregator =
    aggregateEventsFromParquet(GeneralAggregations.aggregateCount) _
  def aggregateFromParquet:ParquetEventsAggregator =
    aggregateEventsFromParquet(GeneralAggregations.aggregateLength) _

  def aggregateEventsFromParquet[A <: SpecificRecord]
  (aggregator:MenthalEventsAggregator)
  (toMenthalEvent:A ⇒ MenthalEvent)
  (datadir: String, eventType: Int, granularity: TimePeriod, outputName: String, sc: SparkContext)
  (implicit ct:ClassTag[A]) = {

    val events = ParquetIO.readEventType[A](datadir, eventType, sc).map(toMenthalEvent)
    val aggregates = aggregator(events, granularity)
    ParquetIO.write(sc, aggregates.map(_.toAvro), datadir + "/" + outputName + Granularity.asString(granularity), AggregationEntry.getClassSchema)
  }
}





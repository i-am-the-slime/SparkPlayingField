package org.menthal.aggregations.tools

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.menthal.aggregations.tools.GeneralAggregators.{MenthalDatasetAggregator, MenthalEventsAggregator}
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

case class AggrDatasetSpec[T<:MenthalEvent](eventType: Int, aggregators: List[(MenthalDatasetAggregator[T], String)])

object AggrSpec {
  def apply[A <: SpecificRecord](eventType: Int, converter: A ⇒ MenthalEvent, aggs: (MenthalEventsAggregator, String)*): AggrSpec[A] = {
    AggrSpec(eventType, converter, aggregators = aggs.toList)
  }

  def count(name: String) = ( GeneralAggregators.aggregateCount, name)

  def duration(name: String) = (GeneralAggregators.aggregateDuration, name)

  def length(name: String) = (GeneralAggregators.aggregateLength, name)

  def countAndDuration(nameCount: String, nameDuration: String) = List(count(nameCount), duration(nameDuration))

  def countAndLength(nameCount: String, nameLength: String) = List(count(nameCount), length(nameLength))







}






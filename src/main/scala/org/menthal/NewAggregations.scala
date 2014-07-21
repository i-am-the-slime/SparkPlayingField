package org.menthal

import org.apache.spark.{Partitioner, SparkContext}
import org.menthal.AppSessionMonoid._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.twitter.algebird.Operators._
import org.joda.time.DateTime
import org.menthal.model.events.MenthalEvent
import org.menthal.model.scalaevents.adapters.PostgresDump

object NewAggregations {
  def main(args:Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NewAggregations dumpFile")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Aggregations", System.getenv("SPARK_HOME"))//, SparkContext.jarOfClass(this.getClass))
    val dumpFile = args(1)
    val eventsDump = sc.textFile(dumpFile,2)
    val events = linesToEvents(eventsDump)
    reduceToAppContainers(events)
    sc.stop()
  }

  def linesToEvents(lines:RDD[String]):RDD[MenthalEvent] =
    lines.flatMap(PostgresDump.tryToParseLineFromDump)

  def reduceToAppContainers(events:RDD[MenthalEvent]):RDD[Pair[Long, AppSessionContainer]] = {
    val containers: RDD[Pair[Pair[Long, Long],AppSessionContainer]] = for {
      event <- events if AppSessionContainer.handledEvents.contains(event.getClass)
      time = event.time
      user = event.userId
      container = AppSessionContainer(event)
    } yield ((time, user), container)

    val sortedAndGrouped = containers.sortByKey().map{case ((time,user), container) => (user,container)}
    sortedAndGrouped.reduceByKey( _ + _ )
  }
}

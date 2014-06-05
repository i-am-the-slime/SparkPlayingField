package org.menthal

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import spray.json._
import DefaultJsonProtocol._
import org.joda.time.DateTime
import com.twitter.algebird.Operators._
import scala.util.{Failure, Success, Try}
import org.menthal.EventData
import org.apache.spark.rdd.RDD
import org.menthal

/**
 * Created by mark on 18.05.14.
 */
object Aggregations {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: Aggregations <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Aggregations", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val dumpFile = "/data"
    val eventsDump = sc.textFile(dumpFile, 2)
    //    aggregate(eventsDump, marksFilter)
    sc.stop()
  }


  def getEventsFromLines(lines: RDD[String], filter: Event[_ <: EventData] => Boolean): RDD[Event[_ <: EventData]] = {
    for {
      line <- lines
      event <- Event.tryToParseLine(line)
      if filter(event)
    } yield event
  }

  def receivedSmsFilter(event: Event[_ <: EventData]): Boolean =
    event.data.eventType == Event.TYPE_SMS_RECEIVED


  //def aggregate(lines: RDD[String], filter: Event[_ <: EventData] => Boolean): RDD[(((Long, DateTime), Map[String, Int]))] = {
  //  val points = getEventsFromLines(lines, filter)


  type UserBucketsRDD[A] = RDD[(((Long, DateTime), A))]
  type MapsShit[A] = UserBucketsRDD[Map[String, A]]
  type EventPredicate[A] = Event[A] => Boolean


  def toSomeMap[B, A <: MappableEventData[B]](events: RDD[Event[A]]): UserBucketsRDD[Map[String, B]] = {
    val buckets = events.map {
      case e: Event[A] =>
        ((e.userId, roundTime(e.time)), e.data.toMap)
    }
    buckets reduceByKey (_ + _)
  }

  def roundTime(time: DateTime): DateTime = {
    time.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
  }

}


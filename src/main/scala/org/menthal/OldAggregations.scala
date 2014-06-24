package org.menthal

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import spray.json._
import DefaultJsonProtocol._
import org.joda.time.DateTime
import com.twitter.algebird.Operators._
import scala.util.{Failure, Success, Try}
import org.apache.spark.rdd.RDD
import com.twitter.algebird.Semigroup

//import scala.math.Numeric.Implicits

/**
 * Created by mark on 18.05.14.
 */
object OldAggregations {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: Aggregations <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Aggregations", System.getenv("SPARK_HOME"))
    val dumpFile = "/data"
    val eventsDump = sc.textFile(dumpFile, 2)
    //    aggregate(eventsDump, marksFilter)
    sc.stop()
  }


  def getEventsFromLines(lines: RDD[String], filter: Event => Boolean): RDD[Event] = {
    for {
      line <- lines
      event <- Event.tryToParseLine(line)
      if filter(event)
    } yield event
  }

  def receivedSmsFilter(event: Event): Boolean =
    event.data.eventType == Event.TYPE_SMS_RECEIVED

  //def aggregate(lines: RDD[String], filter: Event[_ <: EventData] => Boolean): RDD[(((Long, DateTime), Map[String, Int]))] = {
  //  val events = getEventsFromLines(lines, filter)
  //  toMapOne(events)
  //}

  type UserBucketsRDD[A] = RDD[(((Long, DateTime), A))]
  //type MapsShit[A] = UserBucketsRDD[Map[String, A]]
  //type EventPredicate[A] = Event[A] => Boolean

  def toSomeMap[A <: MappableEventData[B], B:Semigroup](events: RDD[Event]): UserBucketsRDD[Map[String, B]] = {
    val buckets = events.map {
      case e: Event => e.data match {
        case d:A => ((e.userId, roundTime(e.time)), d.toMap)
      }
    }
    buckets reduceByKey (_ + _)
  }


  //def getMap[A <: MappableEventData](e:Event[A]): Map[String,Float] =
  //  e.data.toMap

  //def toMapOne[A <: MappableEventData](events: RDD[Event[A]]): UserBucketsRDD[Map[String, Float]] =
  //  toSomeMap(getMap, events)

  def toCounterMap[B, A <: MappableEventData[B]](events: RDD[Event]): UserBucketsRDD[Map[String, Int]] = {
    val buckets = events.map {
      case e: Event => e.data match {
        case d:A => ((e.userId, roundTime(e.time)), d.toCountingMap)
      }
    }
    buckets reduceByKey (_ + _)
  }

  def roundTime(time: DateTime): DateTime = {
    time.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
  }
}


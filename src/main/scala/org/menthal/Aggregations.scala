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

  def main(args:Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: Aggregations <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Aggregations", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val dumpFile = "/data"
    val eventsDump = sc.textFile(dumpFile,2)
//    aggregate(eventsDump, marksFilter)
    sc.stop()
  }


//  def getEventsFromLines(lines:RDD[String], filter: Event[_ <: EventData] => Boolean):RDD[Event[_ <: EventData]] = {
//    for {
//      line <- lines
//      event <- cookEvent(line.split("\t"))
//      if filter(event)
//    } yield event
//  }

  def marksFilter(event: Event[ _ <: EventData ]):Boolean =
    event.data.eventType == Event.TYPE_SCREEN_UNLOCK

//  def aggregate(lines:RDD[String], filter: Event[_ <: EventData] => Boolean):RDD[(((Long, DateTime), Map[String, Int]))] = {
//    val points = getEventsFromLines(lines, filter)
//    //TODO: Maybe generalize more.
//    val buckets = points.map {
//      case e:Event[MarkEventOne] =>
//        ((e.userId, roundTime(e.time)), Map("points" -> e.data.points))
//    }
//    buckets reduceByKey (_ + _)
//  }

  case class Begin(time:DateTime, appName:String)
  case class End(time:DateTime)

//  def aggregateAppSessions(lines:RDD[String]):RDD[AppSession] = {
//    def filter(e: Event[_ <: EventData]):Boolean = {
//      List(
//      Event.TYPE_SCREEN_LOCK,
//      Event.TYPE_SCREEN_UNLOCK,
//      Event.TYPE_WINDOW_STATE_CHANGED
//      ).contains(e.data.eventType)
//    }
//    val events = getEventsFromLines(lines, filter)
//    events.flatMap{
//      case b:Event[ScreenUnlock] => Begin(b.time, b.data)
//      case e:Event[ScreenLock] =>
//      case c:Event[WindowStateChanged] => List()
//    }
//    val appSessions = events.filter(_data.eventType ==)
//  }

  def roundTime(time:DateTime): DateTime = {
    time.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
  }


//  def cookEvent(rawData:Seq[String]):Option[Event[_ <: EventData]] = {
//    val event = for {
//      eventData <-  Try(getEventDataType(rawData(3), rawData(4)))
//      id <- Try(rawData(0).toLong)
//      userId <- Try(rawData(1).toLong)
//      time <- Try(DateTime.parse(rawData(2).replace(" ", "T")))
//      data <- Try(eventData.get)
//     } yield Some(Event[data.type](id, userId, data, time))
//    event getOrElse None
//  }



//  def getEventDataType(typeString:String, data:String):Option[EventData] = {
//    val typeNumber = typeString.toInt
//    typeNumber match {
//      case Event.TYPE_SCREEN_LOCK =>
//        Some(ScreenOff())
//      case Event.TYPE_SCREEN_UNLOCK =>
//        Some(ScreenUnlock())
//      case Event.TYPE_MARK_EVENT_ONE =>
//        val d = data.parseJson.convertTo[Map[String, Int]]
//        Some(MarkEventOne(d.get("points").getOrElse(0)))
//      case Event.TYPE_WINDOW_STATE_CHANGED =>
//        val d = data.parseJson.convertTo[List[String]]
//        Some(WindowStateChanged(d(0), d(1), d(2)))
//      case _ => None
//    }
//  }

}

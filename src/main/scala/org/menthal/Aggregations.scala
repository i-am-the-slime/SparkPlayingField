package org.menthal
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import spray.json._
import DefaultJsonProtocol._
import org.joda.time.DateTime
import com.twitter.algebird.Operators._
import scala.util.{Failure, Success, Try}
import org.apache.spark.rdd.RDD

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
    aggregate(eventsDump)
    sc.stop()
  }

  def aggregate(lines:RDD[String]):RDD[(((Long, DateTime), Map[String, Int]))] = {
    val events = lines.flatMap(e => cookEvent(e.split("\t")))
    val points = events.filter(_.data.eventType == Event.TYPE_MARK_EVENT_ONE)
    val buckets = points.map {
      case e:Event[MarkEventOne] =>
        ((e.userId, roundTime(e.time)), Map("points" -> e.data.points))
    }
    buckets.reduceByKey(_ + _)
  }

  def roundTime(time:DateTime): DateTime = {
    time.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
  }


  def cookEvent(rawData:Seq[String]):Option[Event[_ <: EventData]] = {
    val event = for {
      eventData <-  Try(getEventDataType(rawData(3), rawData(4)))
      id <- Try(rawData(0).toLong)
      userId <- Try(rawData(1).toLong)
      time <- Try(DateTime.parse(rawData(2).replace(" ", "T")))
      data <- Try(eventData.get)
     } yield Some(Event[data.type](id, userId, data, time))
    event getOrElse None
  }



  def getEventDataType(typeString:String, data:String):Option[EventData] = {
    val typeNumber = typeString.toInt
    typeNumber match {
      case Event.TYPE_SCREEN_LOCK =>
        Some(ScreenLock())
      case Event.TYPE_SCREEN_UNLOCK =>
        Some(ScreenUnlock())
      case Event.TYPE_MARK_EVENT_ONE =>
        val d = data.parseJson.convertTo[Map[String, Int]]
        Some(MarkEventOne(d.get("points").getOrElse(0)))
      case Event.TYPE_WINDOW_STATE_CHANGED =>
        val d = data.parseJson.convertTo[List[String]]
        Some(WindowStateChanged(d(0), d(1), d(2)))
      case _ => None
    }
  }

}

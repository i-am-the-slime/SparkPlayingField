package org.menthal
import org.apache.spark.SparkContext
import Event._
import spray.json._
import DefaultJsonProtocol._
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import scala.util.Try

/**
 * Created by mark on 18.05.14.
 */
object MarksAppSessionAggregations {

  def main(args:Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: AppSessions <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "AppSessions",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val dumpFile = "/data"
    val eventsDump = sc.textFile(dumpFile,2)
    val events = eventsDump.flatMap(rawEvent => cookEvent(rawEvent.split("\t")))
    val someEvents = events.sample(withReplacement = false, 0.01, 12)
    System.err.println(someEvents.collect())
//    appSessions.saveAsTextFile("/results.txt")
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    sc.stop()
  }

  def cookEvent(rawData:Seq[String]):Option[Event] = {
    val eventData = getEventDataType(rawData(3), rawData(4))
    if(eventData.isDefined){
      val id = rawData(0).toLong
      val userId = rawData(1).toLong
      val time = DateTime.parse(rawData(2))
      Some(Event(id, userId, eventData.get, time))
    }
    else
      None
  }

  def getEventDataType(typeString:String, data:String):Option[EventData] = {
    val typeNumber = typeString.toInt
    typeNumber match {
        case Event.TYPE_SCREEN_LOCK =>
          Some(ScreenLock())
        case Event.TYPE_SCREEN_UNLOCK =>
          Some(ScreenUnlock())
        case Event.TYPE_WINDOW_STATE_CHANGED =>
          val d = data.parseJson.convertTo[List[String]]
          Some(WindowStateChanged(d(0), d(1), d(2)))
        case _ => None
    }
  }
}

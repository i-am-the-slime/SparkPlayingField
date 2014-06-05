package org.menthal

import org.joda.time.DateTime
import spray.json._
import DefaultJsonProtocol._
import scala.util.Try

/**
 * Created by mark on 18.05.14.
 */
sealed abstract class EventData(val eventType:Long)

case class ScreenOff() extends EventData(Event.TYPE_SCREEN_OFF)
case class ScreenUnlock() extends EventData(Event.TYPE_SCREEN_UNLOCK)
case class DreamingStopped() extends EventData(Event.TYPE_DREAMING_STOPPED)
case class DreamingStarted() extends EventData(Event.TYPE_DREAMING_STARTED)

case class WindowStateChanged(appName:String, packageName:String, windowTitle:String)
  extends EventData(Event.TYPE_WINDOW_STATE_CHANGED)

case class Event[A <: EventData](id:Long, userId:Long, data:A, time:DateTime){

  override def toString:String = {
    val dataString = data.toString
    s"Event: id: $id, user: $userId, time: $time, data: $dataString)"
  }
}
object Event{
  def tryToParseLine(dumpLine: String): Option[Event[_ <: EventData]] = {
    val rawData = dumpLine.split("\t")
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
       case Event.TYPE_SCREEN_OFF =>
         Some(ScreenOff())
       case Event.TYPE_SCREEN_UNLOCK =>
         Some(ScreenUnlock())
       case Event.TYPE_DREAMING_STARTED =>
         Some(DreamingStarted())
       case Event.TYPE_DREAMING_STOPPED =>
         Some(DreamingStopped())
       case Event.TYPE_APP_SESSION =>
         None
       case a if a == Event.TYPE_WINDOW_STATE_CHANGED || a == Event.TYPE_WINDOW_STATE_CHANGE_BASIC =>
         val modifiedData = data.substring(1, data.length()-1).replace("\\","")
         val d = modifiedData.parseJson.convertTo[List[String]]
         Some(WindowStateChanged(d(0), d(1).split("/")(0), d(2)))
       case _ =>
         None
     }
   }
//  def fromJSON(json:String):Option[Event] = {
//
//    val parsed = json.parseJson.convertTo[Map[String, Int]]
//  }
//  val TYPE_VIEW_CLICKED = 1
//  val TYPE_VIEW_LONG_CLICKED = 2
//  val TYPE_VIEW_SELECTED = 4
//  val TYPE_VIEW_FOCUSED = 8
//  val TYPE_VIEW_TEXT_CHANGED = 16
  val TYPE_WINDOW_STATE_CHANGED: Int = 32
//  val TYPE_NOTIFICATION_STATE_CHANGED = 64
//  val TYPE_VIEW_HOVER_ENTER = 128
  val TYPE_WINDOW_STATE_CHANGE_BASIC = 132
//  val TYPE_APP_SESSION_TEST = 256
//  val TYPE_SMS_RECEIVED = 1000
//  val TYPE_SMS_SENT = 1001
//  val TYPE_CALL_RECEIVED = 1002
//  val TYPE_CALL_OUTGOING = 1003
//  val TYPE_CALL_MISSED = 1004
//  val TYPE_SCREEN_ON = 1005
  val TYPE_SCREEN_OFF = 1006
//  val TYPE_LOCALISATION = 1007
//  val TYPE_APP_LIST = 1008
//  val TYPE_APP_INSTALL = 1009
//  val TYPE_APP_REMOVAL = 1010
//  val TYPE_MOOD = 1011
//  val TYPE_PHONE_BOOT = 1012
//  val TYPE_PHONE_SHUTDOWN = 1013
  val TYPE_SCREEN_UNLOCK = 1014
//  val TYPE_APP_UPDATE = 1015
//  val TYPE_ACCESSIBILITY_SERVICE_UPDATE = 1016
  val TYPE_DREAMING_STARTED = 1017
  val TYPE_DREAMING_STOPPED = 1018
//  val TYPE_WHATSAPP_SENT = 1019
//  val TYPE_WHATSAPP_RECEIVED = 1020
//  val TYPE_DEVICE_FEATURES = 1021
//  val TYPE_MENTHAL_APP_ACTION = 1022
//  val TYPE_TIMEZONE = 1023
//  val TYPE_TRAFFIC_DATA = 1025
  val TYPE_APP_SESSION = 1032
//  val TYPE_QUESTIONNAIRE = 1100
//  val TYPE_UNKNOWN = 999999
}

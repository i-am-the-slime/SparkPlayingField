package org.menthal.model.events.adapters

import org.joda.time.DateTime
import org.menthal.model.events._
import org.menthal.model.events.EventData._
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.util.Try

object PostgresDump {
  def tryToParseLineFromDump(dumpLine: String): Option[Event] = {
    val rawData = dumpLine.split("\t")
    val event = for {
      eventData <-  Try(getEventDataType(rawData(3), rawData(4)))
      id <- Try(rawData(0).toLong)
      userId <- Try(rawData(1).toLong)
      time <- Try(DateTime.parse(rawData(2).replace(" ", "T")).getMillis)
      data <- Try(eventData.get)
    } yield Some(Event(id, userId, time, data))
    event getOrElse None
  }

  def getEventDataType(typeString:String, data:String):Option[EventData] = {
    def parsedJSONDataAsList:List[String] = {
      //Data parsed from JSON into a list
      data.substring(1, data.length()-1).replace("\\","").parseJson.convertTo[List[String]]
    }

    lazy val ld = parsedJSONDataAsList
    val typeNumber = typeString.toInt
    typeNumber match {
      case TYPE_SCREEN_ON =>
        Some(ScreenOn())
      case TYPE_SCREEN_OFF =>
        Some(ScreenOff())
      case TYPE_SCREEN_UNLOCK =>
        Some(ScreenUnlock())
      case TYPE_DREAMING_STARTED =>
        Some(DreamingStarted())
      case TYPE_DREAMING_STOPPED =>
        Some(DreamingStopped())
      case TYPE_APP_SESSION =>
        None
      case TYPE_SMS_RECEIVED =>
        Some(SmsReceived(ld(0), ld(1).toInt))
      case TYPE_SMS_SENT =>
        Some(SmsSent(ld(0), ld(1).toInt))
      case TYPE_CALL_RECEIVED =>
        Some(CallReceived(ld(0), ld(1).toLong, ld(2).toLong))
      case TYPE_CALL_OUTGOING =>
        Some(CallOutgoing(ld(0), ld(1).toLong, ld(2).toLong))
      case TYPE_CALL_MISSED =>
        Some(CallMissed(ld(0), ld(1).toLong))
      case TYPE_LOCALISATION =>
        Some(Localisation(ld(0), ld(1).toFloat, ld(2).toDouble, ld(3).toDouble))
      case TYPE_APP_LIST =>
        val d = data.substring(1, data.length()-1).replace("\\","").parseJson.convertTo[List[Map[String,String]]]
        val list:List[AppListItem] = d.flatMap( dict =>
          for {
            pkgName <- dict.get("pkg")
            appName <- dict.get("appName")
          } yield new AppListItem(pkgName, appName) )
        Some(AppList(list))
      case TYPE_APP_INSTALL =>
        Some(AppInstall(ld(0), ld(1)))
      case TYPE_APP_REMOVAL =>
        Some(AppRemoval(ld(0), ld(1)))
      case TYPE_PHONE_BOOT =>
        Some(PhoneBoot())
      case TYPE_PHONE_SHUTDOWN =>
        Some(PhoneShutdown())
      case TYPE_MOOD =>
        Some(Mood(ld(0).toFloat))

      case TYPE_WHATSAPP_SENT =>
        val ld = data.substring(1, data.length()-1).replace("\\","").parseJson.asInstanceOf[JsArray].elements
        Some(WhatsAppSent(ld(0).convertTo[String], ld(1).convertTo[Int], if(ld(2).convertTo[Int] == 0) false else true))
      case TYPE_WHATSAPP_RECEIVED =>
        val ld = data.substring(1, data.length()-1).replace("\\","").parseJson.asInstanceOf[JsArray].elements
        Some(WhatsAppReceived(ld(0).convertTo[String], ld(1).convertTo[Int], if(ld(2).convertTo[Int] == 0) false else true))
      case TYPE_QUESTIONNAIRE =>
        Some(Questionnaire(ld(0).toInt, ld(1).toInt, ld(2).toInt))
      case a if a == TYPE_WINDOW_STATE_CHANGED || a == TYPE_WINDOW_STATE_CHANGED_BASIC =>
        Some(WindowStateChanged(ld(0), ld(1).split("/")(0), ld(2)))
      case _ =>
        None
    }
  }
}
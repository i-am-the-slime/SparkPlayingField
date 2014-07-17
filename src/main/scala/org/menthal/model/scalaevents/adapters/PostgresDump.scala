package org.menthal.model.scalaevents.adapters

import org.joda.time.DateTime
import org.menthal.model.events._
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.util.Try

object PostgresDump {
  def tryToParseLineFromDump(dumpLine: String): Option[MenthalEvent] = {
    val rawData = dumpLine.split("\t")
    val event = for {
      theEvent <-  Try(getEvent(rawData(0), rawData(1), rawData(2), rawData(3), rawData(4)))
    } yield theEvent
    event getOrElse None
  }

  def getEvent(idStr:String, userIdStr:String, timeStr:String, typeStr:String, data:String):Option[MenthalEvent] = {
    def parsedJSONDataAsList:List[String] = {
      //Data parsed from JSON into a list
      data.substring(1, data.length()-1).replace("\\","").parseJson.convertTo[List[String]]
    }

    val id = idStr.toLong
    val userId = userIdStr.toLong
    val time = DateTime.parse(timeStr.replace(" ", "T")).getMillis

    lazy val ld = parsedJSONDataAsList
    val typeNumber = typeStr.toInt
    import EventData._
    typeNumber match {
      case TYPE_SCREEN_ON =>
        Some(CCScreenOn(id, userId, time))
      case TYPE_SCREEN_OFF =>
        Some(CCScreenOff(id, userId, time))
      case TYPE_SCREEN_UNLOCK =>
        Some(CCScreenUnlock(id, userId, time))
      case TYPE_DREAMING_STARTED =>
        Some(CCDreamingStarted(id, userId, time))
      case TYPE_DREAMING_STOPPED =>
        Some(CCDreamingStopped(id, userId, time))
      case TYPE_APP_SESSION =>
        None
      case TYPE_SMS_RECEIVED =>
        Some(CCSmsReceived(id, userId, time, ld(0), ld(1).toInt))
      case TYPE_SMS_SENT =>
        Some(CCSmsSent(id, userId, time, ld(0), ld(1).toInt))
      case TYPE_CALL_RECEIVED =>
        Some(CCCallReceived(id, userId, time, ld(0), ld(1).toLong, ld(2).toLong))
      case TYPE_CALL_OUTGOING =>
        Some(CCCallOutgoing(id, userId, time, ld(0), ld(1).toLong, ld(2).toLong))
      case TYPE_CALL_MISSED =>
        Some(CCCallMissed(id, userId, time, ld(0), ld(1).toLong))
      case TYPE_LOCALISATION =>
        Some(CCLocalisation(id, userId, time, ld(0), ld(1).toFloat, ld(2).toDouble, ld(3).toDouble))
//      case TYPE_APP_LIST =>
//        val d = data.substring(1, data.length()-1).replace("\\","").parseJson.convertTo[List[Map[String,String]]]
//        val list:List[AppListItem] = d.flatMap( dict =>
//          for {
//            pkgName <- dict.get("pkg")
//            appName <- dict.get("appName")
//          } yield CCAppListItem(pkgName, appName) )
//        Some(AppList(list))
      case TYPE_APP_INSTALL =>
        Some(CCAppInstall(id, userId, time, ld(0), ld(1)))
      case TYPE_APP_REMOVAL =>
        Some(CCAppRemoval(id, userId, time, ld(0), ld(1)))
      case TYPE_PHONE_BOOT =>
        Some(CCPhoneBoot(id, userId, time))
      case TYPE_PHONE_SHUTDOWN =>
        Some(CCPhoneShutdown(id, userId, time))
      case TYPE_MOOD =>
        Some(CCMood(id, userId, time, ld(0).toFloat))

      case TYPE_WHATSAPP_SENT =>
        val ld = data.substring(1, data.length()-1).replace("\\","").parseJson.asInstanceOf[JsArray].elements
        Some(CCWhatsAppSent(id, userId, time, ld(0).convertTo[String], ld(1).convertTo[Int], if(ld(2).convertTo[Int] == 0) false else true))
      case TYPE_WHATSAPP_RECEIVED =>
        val ld = data.substring(1, data.length()-1).replace("\\","").parseJson.asInstanceOf[JsArray].elements
        Some(CCWhatsAppReceived(id, userId, time, ld(0).convertTo[String], ld(1).convertTo[Int], if(ld(2).convertTo[Int] == 0) false else true))
      case TYPE_QUESTIONNAIRE =>
        Some(CCQuestionnaire(id, userId, time, ld(0).toInt, ld(1).toInt, ld(2).toInt))
      case a if a == TYPE_WINDOW_STATE_CHANGED || a == TYPE_WINDOW_STATE_CHANGED_BASIC =>
        Some(CCWindowStateChanged(id, userId, time, ld(0), ld(1).split("/")(0), ld(2)))
      case _ =>
        None
    }
  }

  object EventData {
    val TYPE_WINDOW_STATE_CHANGED = 32
    val TYPE_WINDOW_STATE_CHANGED_BASIC = 132
    val TYPE_SMS_RECEIVED = 1000
    val TYPE_SMS_SENT = 1001
    val TYPE_CALL_RECEIVED = 1002
    val TYPE_CALL_OUTGOING = 1003
    val TYPE_CALL_MISSED = 1004
    val TYPE_SCREEN_ON = 1005
    val TYPE_SCREEN_OFF = 1006
    val TYPE_LOCALISATION = 1007
    val TYPE_APP_LIST = 1008
    val TYPE_APP_INSTALL = 1009
    val TYPE_APP_REMOVAL = 1010
    val TYPE_MOOD = 1011
    val TYPE_PHONE_BOOT = 1012
    val TYPE_PHONE_SHUTDOWN = 1013
    val TYPE_SCREEN_UNLOCK = 1014
    val TYPE_DREAMING_STARTED = 1017
    val TYPE_DREAMING_STOPPED = 1018
    val TYPE_WHATSAPP_SENT = 1019
    val TYPE_WHATSAPP_RECEIVED = 1020
    val TYPE_DEVICE_FEATURES = 1021
    val TYPE_MENTHAL_APP_ACTION = 1022
    val TYPE_TIMEZONE = 1023
    val TYPE_TRAFFIC_DATA = 1025
    val TYPE_APP_SESSION = 1032
    val TYPE_QUESTIONNAIRE = 1100
    //  val TYPE_VIEW_CLICKED = 1
    //  val TYPE_VIEW_LONG_CLICKED = 2
    //  val TYPE_VIEW_SELECTED = 4
    //  val TYPE_VIEW_FOCUSED = 8
    //  val TYPE_VIEW_TEXT_CHANGED = 16
    //  val TYPE_NOTIFICATION_STATE_CHANGED = 64
    //  val TYPE_VIEW_HOVER_ENTER = 128
    //  val TYPE_APP_SESSION_TEST = 256
    //  val TYPE_APP_UPDATE = 1015
    //  val TYPE_ACCESSIBILITY_SERVICE_UPDATE = 1016
    //  val TYPE_UNKNOWN = 999999
  }
}
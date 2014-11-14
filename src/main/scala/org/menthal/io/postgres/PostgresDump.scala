package org.menthal.io.postgres

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.menthal.model.events._
import org.menthal.model.EventType._
import spray.json.DefaultJsonProtocol._
import spray.json._


import scala.util.Try

object PostgresDump {

  def parseDumpFile(sc: SparkContext, dumpFilePath: String): RDD[_ <: MenthalEvent] = {
    for {
      line <- sc.textFile(dumpFilePath)
      event <- PostgresDump.tryToParseLineFromDump(line)
    } yield event
  }

  def tryToParseLineFromDump(dumpLine: String): Option[MenthalEvent] = {
    val rawData = dumpLine.split("\t")
    val event = for {
      theEvent <- Try(getEvent(rawData(0), rawData(1), rawData(2), rawData(3), rawData(4)))
    } yield theEvent
    event getOrElse None
  }

  def getEvent(idStr: String, userIdStr: String, timeStr: String, typeStr: String, data: String): Option[MenthalEvent] = {
    def parsedJSONDataAsList: List[String] = {
      //Data parsed from JSON into a list
      data.substring(1, data.length() - 1).replace("\\", "").parseJson.convertTo[List[String]]
    }

    val id = idStr.toLong
    val userId = userIdStr.toLong
    val time = DateTime.parse(timeStr.replace(" ", "T")).getMillis

    lazy val ld = parsedJSONDataAsList
    val typeNumber = typeStr.toInt
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
      case TYPE_APP_LIST =>
             val d = data.substring(1, data.length()-1).replace("\\","").parseJson.convertTo[List[Map[String,String]]]
             val list:List[String] = d.map(dict => dict.getOrElse("pkg",""))
             Some(CCAppList(id, userId, time, list.toBuffer))
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
        val ld = data.substring(1, data.length() - 1).replace("\\", "").parseJson.asInstanceOf[JsArray].elements
        Some(CCWhatsAppSent(id, userId, time, ld(0).convertTo[String], ld(1).convertTo[Int], if (ld(2).convertTo[Int] == 0) false else true))
      case TYPE_WHATSAPP_RECEIVED =>
        val ld = data.substring(1, data.length() - 1).replace("\\", "").parseJson.asInstanceOf[JsArray].elements
        Some(CCWhatsAppReceived(id, userId, time, ld(0).convertTo[String], ld(1).convertTo[Int], if (ld(2).convertTo[Int] == 0) false else true))
      case TYPE_QUESTIONNAIRE =>
        Some(CCQuestionnaire(id, userId, time, ld.head.toInt, ld.tail.toBuffer))
      case a if a == TYPE_WINDOW_STATE_CHANGED || a == TYPE_WINDOW_STATE_CHANGED_BASIC =>
        Some(CCWindowStateChanged(id, userId, time, ld(0), ld(1).split("/")(0), ld(2)))
      case _ =>
        None
    }
  }
}

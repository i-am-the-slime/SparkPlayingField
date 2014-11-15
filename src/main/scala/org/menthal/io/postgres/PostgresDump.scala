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

    def parsedJSONDataAsStringList: List[String] = {
      if (data.startsWith("\""))
        data.substring(1, data.length() - 1).replace("\\", "").parseJson.convertTo[List[String]]
      else
        data.parseJson.convertTo[List[String]]
    }

    def parsedJSONDataAsJSValueList: List[JsValue] = {
      if (data.startsWith("\""))
        data.substring(1, data.length() - 1).replace("\\", "").parseJson.asInstanceOf[JsArray].elements
      else
        data.parseJson.asInstanceOf[JsArray].elements
    }

    val id = idStr.toLong
    val userId = userIdStr.toLong
    val time = DateTime.parse(timeStr.replace(" ", "T")).getMillis

    lazy val ld = parsedJSONDataAsStringList
    lazy val jd = parsedJSONDataAsJSValueList
    val typeNumber = typeStr.toInt
    typeNumber match {
      case TYPE_APP_INSTALL =>
        ld match {
          case appName::packageName::Nil =>
            Some(CCAppInstall(id, userId, time, appName, packageName))
          case _ => None
        }
      case TYPE_APP_LIST =>
        val d = data.substring(1, data.length()-1).replace("\\","").parseJson.convertTo[List[Map[String,String]]]
        val list:List[String] = d.map(dict => dict.getOrElse("pkg",""))
        Some(CCAppList(id, userId, time, list.toBuffer))
      case TYPE_APP_REMOVAL =>
        ld match {
          case appName :: packageName :: Nil => Some(CCAppRemoval(id, userId, time, appName, packageName))
          case _ => None
        }
      case TYPE_APP_SESSION =>
        ld match {
          case startTime ::duration ::appName :: packageName :: Nil =>
            Some(CCAppSession(userId, time, duration.toLong, packageName))
          case _ => None
        }
      case TYPE_APP_UPGRADE =>
        Some(CCAppUpgrade(id, userId, time))
      case TYPE_CALL_MISSED =>
        ld match {
          case contactHash :: timestamp :: Nil => Some(CCCallMissed(id, userId, time, contactHash, timestamp.toLong))
          case _ => None
        }
        Some(CCCallMissed(id, userId, time, ld(0), ld(1).toLong))
      case TYPE_CALL_OUTGOING =>
        ld match {
          case contactHash :: startTimestamp ::durationInMillis :: Nil =>
            Some(CCCallOutgoing(id, userId, time, contactHash, startTimestamp.toLong, durationInMillis.toLong))
          case _ => None
        }
      case TYPE_CALL_RECEIVED =>
        ld match {
          case contactHash :: startTimestamp ::durationInMillis :: Nil =>
            Some(CCCallReceived(id, userId, time, contactHash, startTimestamp.toLong, durationInMillis.toLong))
          case _ => None
        }
      case TYPE_DEVICE_FEATURES =>
        ld match {
          case androidVersion :: deviceName ::operatorName :: Nil =>
            Some(CCDeviceFeatures(id, userId, time, androidVersion, deviceName, operatorName))
          case _ => None
        }
      case TYPE_DREAMING_STARTED =>
        Some(CCDreamingStarted(id, userId, time))
      case TYPE_DREAMING_STOPPED =>
        Some(CCDreamingStopped(id, userId, time))
      case TYPE_LOCALISATION =>
        ld match {
          case signalType :: accuracy :: lng :: lat :: Nil =>
            Some(CCLocalisation(id, userId, time, signalType, accuracy.toFloat, lng.toDouble, lat.toDouble))
          case _ => None
        }
      case TYPE_MENTHAL_APP_ACTION =>
        ld match {
          case fragment :: Nil => Some(CCMenthalAppEvent(id, userId, time, fragment))
          case _ => None
        }
      case TYPE_MOOD =>
        ld match {
          case moodValue :: moodDescription :: Nil =>
            Some(CCMood(id, userId, time, moodValue.toFloat)) //TODO use moodDescription
          case _ => None
        }
      case TYPE_NOTIFICATION_STATE_CHANGED =>
        jd match {
          case appName :: packageName :: length :: Nil =>
            Some(CCNotificationStateChanged(id, userId, time, appName.convertTo[String], packageName.convertTo[String], length.convertTo[Long]))
          case _ => None
        }
      case TYPE_PHONE_BOOT =>
        Some(CCPhoneBoot(id, userId, time))
      case TYPE_PHONE_SHUTDOWN =>
        Some(CCPhoneShutdown(id, userId, time))
      case TYPE_SCREEN_ON =>
        Some(CCScreenOn(id, userId, time))
      case TYPE_SCREEN_OFF =>
        Some(CCScreenOff(id, userId, time))
      case TYPE_SCREEN_UNLOCK =>
        Some(CCScreenUnlock(id, userId, time))
      case TYPE_SMS_RECEIVED =>
        ld match {
          case contactHash :: msgLength :: Nil =>
            Some(CCSmsReceived(id, userId, time, contactHash, msgLength.toInt))
          case _ => None
        }
      case TYPE_SMS_SENT =>
        ld match {
          case contactHash :: msgLength :: Nil =>
            Some(CCSmsSent(id, userId, time, contactHash, msgLength.toInt))
          case _ => None
       }
      case TYPE_TIMEZONE =>
        ld match {
          case moTime :: momoTime :: offsetInMillis :: Nil =>
            Some(CCTimeZone(id, userId, time, moTime.toLong, momoTime.toLong, offsetInMillis.toLong))
          case _ => None
        }
      case TYPE_TRAFFIC_DATA =>
        ld match {
          case connectionType :: name :: networkType :: bytesReceived :: bytesTransferred:: bytesTransferredOverMobile :: smth :: Nil =>
            Some(CCTrafficData(id, userId, time, connectionType.toInt, name, networkType.toInt,
              bytesReceived.toLong, bytesTransferred.toLong, bytesTransferredOverMobile.toLong))
          case _ => None
        }
      case TYPE_WHATSAPP_SENT =>
        jd match {
          case contactHash :: msgLength :: groupMsgIndicator :: Nil =>
            val isGroupMsg = if (groupMsgIndicator.convertTo[Int] == 0) false else true
            Some(CCWhatsAppSent(id, userId, time, contactHash.convertTo[String], msgLength.convertTo[Int], isGroupMsg))
          case _ => None
        }
      case TYPE_WHATSAPP_RECEIVED =>
        jd match {
          case contactHash :: msgLength :: groupMsgIndicator :: Nil =>
            val isGroupMsg = if (groupMsgIndicator.convertTo[Int] == 0) false else true
            Some(CCWhatsAppReceived(id, userId, time, contactHash.convertTo[String], msgLength.convertTo[Int], isGroupMsg))
          case _ => None
        }
      case a if a == TYPE_WINDOW_STATE_CHANGED || a == TYPE_WINDOW_STATE_CHANGED_BASIC =>
        ld match {
          case appName :: fullPackageName :: windowTitle :: Nil =>
            val packageName = fullPackageName.split("/")(0)
            Some(CCWindowStateChanged(id, userId, time, appName, packageName, windowTitle))
          case _ => None
        }
      case TYPE_QUESTIONNAIRE =>
        ld match {
          case questionnaireVersion :: answers =>
           Some(CCQuestionnaire(id, userId, time, questionnaireVersion.toInt, answers.toBuffer))
          case _ => None
        }
      case _ =>
        None
    }
  }
}

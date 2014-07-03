package org.menthal.model.events

import EventData._

sealed abstract class EventData(val eventType:Long)

trait MappableEventData[A] {
  def toMap:Map[String, A]
  def toCountingMap:Map[String, Int]
}

case class WindowStateChanged(appName:String, packageName:String, windowTitle:String)
  extends EventData(TYPE_WINDOW_STATE_CHANGED)

case class SmsReceived(contactHash:String, msgLength:Int)
            extends EventData(TYPE_SMS_RECEIVED)
            with MappableEventData[Int]  {
  def toMap = Map(contactHash -> msgLength)
  def toCountingMap = Map(contactHash -> 1)
}


/**
 * Documents the sending of an SMS
 * @param contactHash the salted SHA-512 hash of the recipient's phone number string
 * @param msgLength the length of the message in characters
 */
case class SmsSent(contactHash:String, msgLength:Int) extends EventData(TYPE_SMS_SENT)

/**
 * Documents the event of receiving a Call
 * @param contactHash the salted SHA-512 hash of the caller's name or phone number in E164 format (eg. +49228123456)
 * @param startTimestamp beginning of the call as a Unix Timestamp in milliseconds
 * @param durationInMillis the call duration in milliseconds
 */
case class CallReceived(contactHash:String, startTimestamp:Long, durationInMillis:Long)
  extends EventData(TYPE_CALL_RECEIVED)

case class CallOutgoing(contactHash:String, startTimestamp:Long, durationInMillis:Long)
  extends EventData(TYPE_CALL_OUTGOING)

case class CallMissed(contactHash:String, timestamp:Long) extends EventData(TYPE_CALL_MISSED)

case class ScreenOn() extends EventData(TYPE_SCREEN_ON)

case class ScreenOff() extends EventData(TYPE_SCREEN_OFF)

case class Localisation(signalType:String, accuracy:Float, longitude:Double, latitude:Double) extends EventData(TYPE_LOCALISATION)

case class AppListItem(packageName:String, appName:String)
case class AppList(list:Seq[AppListItem]) extends EventData(TYPE_APP_LIST)

case class AppInstall(appName:String, packageName:String) extends EventData(TYPE_APP_INSTALL)

case class AppRemoval(appName:String, packageName:String) extends EventData(TYPE_APP_REMOVAL)

case class Mood(moodValue:Float) extends EventData(TYPE_MOOD)

case class PhoneBoot() extends EventData(TYPE_PHONE_BOOT)

case class PhoneShutdown() extends EventData(TYPE_PHONE_SHUTDOWN)

case class ScreenUnlock() extends EventData(TYPE_SCREEN_UNLOCK)

case class DreamingStopped() extends EventData(TYPE_DREAMING_STOPPED)

case class DreamingStarted() extends EventData(TYPE_DREAMING_STARTED)

case class WhatsAppSent(contactHash:String, messageLength:Int, isGroupMessage:Boolean) extends EventData(TYPE_WHATSAPP_SENT)

case class WhatsAppReceived(contactHash:String, messageLength:Int, isGroupMessage:Boolean) extends EventData(TYPE_WHATSAPP_SENT)

case class DeviceFeatures(androidVersion:String, phoneModel:String, carrier:String)

case class MenthalAppAction(frontMostView:String)

case class TimeZone(offsetInMillis:Long) extends EventData(TYPE_TIMEZONE)

/*
Values for connection type.
-1: No Connection
0: Mobile
1: Wifi
2: MMS
3: Mobile Supl
4: Mobile Dun
5: High Priority

Network type:
  NETWORK_TYPE_UNKNOWN = 0;
  NETWORK_TYPE_GPRS = 1;
  NETWORK_TYPE_EDGE = 2;
  NETWORK_TYPE_UMTS = 3;
  NETWORK_TYPE_CDMA = 4;
  NETWORK_TYPE_EVDO_0 = 5;
  NETWORK_TYPE_EVDO_A = 6;
  NETWORK_TYPE_1xRTT = 7;
  NETWORK_TYPE_HSDPA = 8;
  NETWORK_TYPE_HSUPA = 9;
  NETWORK_TYPE_HSPA = 10;
  NETWORK_TYPE_IDEN = 11;
  NETWORK_TYPE_EVDO_B = 12;
  NETWORK_TYPE_LTE = 13;
  NETWORK_TYPE_EHRPD = 14;
  NETWORK_TYPE_HSPAP = 15;
*/
case class TrafficData(connectionType:Int, name:String, networkType:Int, bytesReceived:Long,
  bytesTransferred:Long, bytesTransferredOverMobile:Long) extends EventData(TYPE_TRAFFIC_DATA)

case class AppSession(startTimestamp:Long, durationInMillis:Long, appName:String, packageName:String)

case class Questionnaire(answer1:Int, answer2:Int, answer3:Int) extends EventData(TYPE_QUESTIONNAIRE)


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

package org.menthal.model.events

  abstract class Alpha { def magic: Double}
  class Beta extends Alpha { val magic = math.Pi }
  case class Gamma(magic: Double) extends Alpha
  case class Delta() extends Beta
  case class Epsilon[T]() extends Beta

sealed abstract class EventData2(val eventType:Long)


case class WindowStateChanged2(appName:String, packageName:String, windowTitle:String)
  extends EventData2(EventData.TYPE_WINDOW_STATE_CHANGED)

case class SmsReceived2(contactHash:String, msgLength:Int)
  extends EventData2(EventData.TYPE_SMS_RECEIVED) {
  def toMap = Map(contactHash -> msgLength)
  def toCountingMap = Map(contactHash -> 1)
}

object EventData {

  //REMOVE FROM HERE

  //REMOVE UNTIL HERE
  sealed abstract class EventData(val eventType:Long)

  trait MappableEventData[A] {
    def toMap:Map[String, A]
    def toCountingMap:Map[String, Int]
  }

  val TYPE_WINDOW_STATE_CHANGED = 32
  val TYPE_WINDOW_STATE_CHANGED_BASIC = 132
  case class WindowStateChanged(appName:String, packageName:String, windowTitle:String)
    extends EventData(TYPE_WINDOW_STATE_CHANGED)

  val TYPE_SMS_RECEIVED = 1000
  case class SmsReceived(contactHash:String, msgLength:Int)
              extends EventData(TYPE_SMS_RECEIVED)
              with MappableEventData[Int]  {
    def toMap = Map(contactHash -> msgLength)
    def toCountingMap = Map(contactHash -> 1)
  }

  val TYPE_SMS_SENT = 1001

  /**
   * Documents the sending of an SMS
   * @param contactHash the salted SHA-512 hash of the recipient's phone number string
   * @param msgLength the length of the message in characters
   */
  case class SmsSent(contactHash:String, msgLength:Int) extends EventData(TYPE_SMS_SENT)

  val TYPE_CALL_RECEIVED = 1002
  /**
   * Documents the event of receiving a Call
   * @param contactHash the salted SHA-512 hash of the caller's name or phone number in E164 format (eg. +49228123456)
   * @param startTimestamp beginning of the call as a Unix Timestamp in milliseconds
   * @param durationInMillis the call duration in milliseconds
   */
  case class CallReceived(contactHash:String, startTimestamp:Long, durationInMillis:Long)
    extends EventData(TYPE_CALL_RECEIVED)

  val TYPE_CALL_OUTGOING = 1003
  case class CallOutgoing(contactHash:String, startTimestamp:Long, durationInMillis:Long)
    extends EventData(TYPE_CALL_OUTGOING)

  val TYPE_CALL_MISSED = 1004
  case class CallMissed(contactHash:String, timestamp:Long) extends EventData(TYPE_CALL_MISSED)

  val TYPE_SCREEN_ON = 1005
  case class ScreenOn() extends EventData(TYPE_SCREEN_ON)

  val TYPE_SCREEN_OFF = 1006
  case class ScreenOff() extends EventData(TYPE_SCREEN_OFF)

  val TYPE_LOCALISATION = 1007
  case class Localisation(signalType:String, accuracy:Float, longitude:Double, latitude:Double) extends EventData(TYPE_LOCALISATION)

  val TYPE_APP_LIST = 1008
  case class AppListItem(packageName:String, appName:String)
  case class AppList(list:Seq[AppListItem]) extends EventData(TYPE_APP_LIST)

  val TYPE_APP_INSTALL = 1009
  case class AppInstall(appName:String, packageName:String) extends EventData(TYPE_APP_INSTALL)

  val TYPE_APP_REMOVAL = 1010
  case class AppRemoval(appName:String, packageName:String) extends EventData(TYPE_APP_REMOVAL)

  val TYPE_MOOD = 1011
  case class Mood(moodValue:Float) extends EventData(TYPE_MOOD)

  val TYPE_PHONE_BOOT = 1012
  case class PhoneBoot() extends EventData(TYPE_PHONE_BOOT)

  val TYPE_PHONE_SHUTDOWN = 1013
  case class PhoneShutdown() extends EventData(TYPE_PHONE_SHUTDOWN)

  val TYPE_SCREEN_UNLOCK = 1014
  case class ScreenUnlock() extends EventData(TYPE_SCREEN_UNLOCK)

  val TYPE_DREAMING_STARTED = 1017
  case class DreamingStopped() extends EventData(TYPE_DREAMING_STOPPED)

  val TYPE_DREAMING_STOPPED = 1018
  case class DreamingStarted() extends EventData(TYPE_DREAMING_STARTED)

  val TYPE_WHATSAPP_SENT = 1019
  case class WhatsAppSent(contactHash:String, messageLength:Int, isGroupMessage:Boolean) extends EventData(TYPE_WHATSAPP_SENT)

  val TYPE_WHATSAPP_RECEIVED = 1020
  case class WhatsAppReceived(contactHash:String, messageLength:Int, isGroupMessage:Boolean) extends EventData(TYPE_WHATSAPP_SENT)

  val TYPE_DEVICE_FEATURES = 1021
  case class DeviceFeatures(androidVersion:String, phoneModel:String, carrier:String)

  val TYPE_MENTHAL_APP_ACTION = 1022
  case class MenthalAppAction(frontMostView:String)

  val TYPE_TIMEZONE = 1023
  case class TimeZone(offsetInMillis:Long) extends EventData(TYPE_TIMEZONE)

  val TYPE_TRAFFIC_DATA = 1025
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

  val TYPE_APP_SESSION = 1032
  case class AppSession(startTimestamp:Long, durationInMillis:Long, appName:String, packageName:String)

  val TYPE_QUESTIONNAIRE = 1100
  case class Questionnaire(answer1:Int, answer2:Int, answer3:Int) extends EventData(TYPE_QUESTIONNAIRE)

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

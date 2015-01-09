package org.menthal.model

import org.apache.avro.Schema
import org.menthal.model.events._

/**
 * Created by konrad on 13.11.14.
 */
  object EventType {
    val TYPE_UNKNOWN = 999999
    val TYPE_VIEW_CLICKED = 1
    val TYPE_VIEW_LONG_CLICKED = 2
    val TYPE_VIEW_SELECTED = 4
    val TYPE_VIEW_FOCUSED = 8
    val TYPE_VIEW_TEXT_CHANGED = 16
    val TYPE_WINDOW_STATE_CHANGED = 32
    val TYPE_NOTIFICATION_STATE_CHANGED = 64
    val TYPE_VIEW_HOVER_ENTER = 128
    val TYPE_WINDOW_STATE_CHANGED_BASIC = 132
    val TYPE_APP_SESSION_TEST = 256
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
    val TYPE_APP_UPGRADE = 1015
    val TYPE_ACCESSIBILITY_SERVICE_UPDATE = 1016
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
    val TYPE_AGGREGATION_ENTRY = 2000

  def fromMenthalEvent(event: MenthalEvent): Int = {
    event match {
      case _ : CCAccessibilityServiceUpdate => TYPE_ACCESSIBILITY_SERVICE_UPDATE
      case _ : CCAggregationEntry ⇒ TYPE_AGGREGATION_ENTRY
      case _ : CCAppInstall => TYPE_APP_INSTALL
      case _ : CCAppList => TYPE_APP_LIST
      case _ : CCAppRemoval => TYPE_APP_REMOVAL
      case _ : CCAppSession => TYPE_APP_SESSION
      case _ : CCAppUpgrade => TYPE_APP_UPGRADE
      case _ : CCCallMissed => TYPE_CALL_MISSED
      case _ : CCCallOutgoing => TYPE_CALL_OUTGOING
      case _ : CCCallReceived => TYPE_CALL_RECEIVED
      case _ : CCDeviceFeatures => TYPE_DEVICE_FEATURES
      case _ : CCDreamingStarted => TYPE_DREAMING_STARTED
      case _ : CCDreamingStopped => TYPE_DREAMING_STOPPED
      case _ : CCLocalisation => TYPE_LOCALISATION
      case _ : CCMenthalAppEvent => TYPE_MENTHAL_APP_ACTION
      case _ : CCMood => TYPE_MOOD
      case _ : CCNotificationStateChanged => TYPE_NOTIFICATION_STATE_CHANGED
      case _ : CCPhoneBoot => TYPE_PHONE_BOOT
      case _ : CCPhoneShutdown => TYPE_PHONE_SHUTDOWN
      case _ : CCScreenOn => TYPE_SCREEN_ON
      case _ : CCScreenOff => TYPE_SCREEN_OFF
      case _ : CCScreenUnlock => TYPE_SCREEN_UNLOCK
      case _ : CCSmsReceived => TYPE_SMS_RECEIVED
      case _ : CCSmsSent => TYPE_SMS_SENT
      case _ : CCTimeZone => TYPE_TIMEZONE
      case _ : CCTrafficData => TYPE_TRAFFIC_DATA
      case _ : CCUnknown => TYPE_UNKNOWN
      case _ : CCWhatsAppSent => TYPE_WHATSAPP_SENT
      case _ : CCWhatsAppReceived => TYPE_WHATSAPP_RECEIVED
      case _ : CCWindowStateChanged => TYPE_WINDOW_STATE_CHANGED
      case _ : CCQuestionnaire => TYPE_QUESTIONNAIRE
      case _ => TYPE_UNKNOWN
    }
  }

  def toPath(eventType: Int): String = {
    eventType match {
      case TYPE_ACCESSIBILITY_SERVICE_UPDATE => "accessibility_service_update"
      case TYPE_AGGREGATION_ENTRY ⇒ "aggregation"
      case TYPE_APP_LIST => "app_list"
      case TYPE_APP_INSTALL => "app_install"
      case TYPE_APP_REMOVAL => "app_removal"
      case TYPE_APP_SESSION => "app_session"
      case TYPE_APP_UPGRADE => "app_upgrade"
      case TYPE_CALL_MISSED => "call_missed"
      case TYPE_CALL_RECEIVED => "call_received"
      case TYPE_CALL_OUTGOING => "call_outgoing"
      case TYPE_DEVICE_FEATURES => "device_features"
      case TYPE_DREAMING_STOPPED => "dreaming_stopped"
      case TYPE_DREAMING_STARTED => "dreaming_started"
      case TYPE_LOCALISATION => "localisation"
      case TYPE_MENTHAL_APP_ACTION => "menthal_app_action"
      case TYPE_MOOD => "mood"
      case TYPE_NOTIFICATION_STATE_CHANGED => "notification_state_change"
      case TYPE_PHONE_BOOT => "phone_boot"
      case TYPE_PHONE_SHUTDOWN => "phone_shutdown"
      case TYPE_QUESTIONNAIRE => "questionnaire"
      case TYPE_SCREEN_OFF => "screen_off"
      case TYPE_SCREEN_ON => "screen_on"
      case TYPE_SCREEN_UNLOCK => "screen_unlock"
      case TYPE_SMS_RECEIVED => "sms_received"
      case TYPE_SMS_SENT => "sms_sent"
      case TYPE_TIMEZONE => "timezone"
      case TYPE_UNKNOWN => "unknown"
      case TYPE_TRAFFIC_DATA => "traffic_data"
      case TYPE_WHATSAPP_RECEIVED => "whatsapp_received"
      case TYPE_WHATSAPP_SENT => "whatsapp_sent"
      case a if a == TYPE_WINDOW_STATE_CHANGED || a == TYPE_WINDOW_STATE_CHANGED_BASIC =>
        "window_state_changed"
      case _ => "unknown"
    }
  }


  def toSchema(eventType: Int):Schema = {//TODO use schema lookup service instead
    eventType match {
      case TYPE_ACCESSIBILITY_SERVICE_UPDATE ⇒ AccessibilityServiceUpdate.getClassSchema
      case TYPE_AGGREGATION_ENTRY ⇒ AggregationEntry.getClassSchema
      case TYPE_APP_INSTALL => AppInstall.getClassSchema
      case TYPE_APP_LIST => AppList.getClassSchema
      case TYPE_APP_REMOVAL => AppRemoval.getClassSchema
      case TYPE_APP_SESSION => AppSession.getClassSchema
      case TYPE_APP_UPGRADE => AppUpgrade.getClassSchema
      case TYPE_ACCESSIBILITY_SERVICE_UPDATE => AccessibilityServiceUpdate.getClassSchema
      case TYPE_CALL_MISSED => CallMissed.getClassSchema
      case TYPE_CALL_OUTGOING => CallOutgoing.getClassSchema
      case TYPE_CALL_RECEIVED => CallReceived.getClassSchema
      case TYPE_DEVICE_FEATURES => DeviceFeatures.getClassSchema
      case TYPE_DREAMING_STARTED => DreamingStarted.getClassSchema
      case TYPE_DREAMING_STOPPED => DreamingStopped.getClassSchema
      case TYPE_LOCALISATION => Localisation.getClassSchema
      case TYPE_MENTHAL_APP_ACTION => MenthalAppEvent.getClassSchema
      case TYPE_MOOD => Mood.getClassSchema
      case TYPE_NOTIFICATION_STATE_CHANGED => NotificationStateChanged.getClassSchema
      case TYPE_PHONE_BOOT => PhoneBoot.getClassSchema
      case TYPE_PHONE_SHUTDOWN => PhoneShutdown.getClassSchema
      case TYPE_SCREEN_ON => ScreenOn.getClassSchema
      case TYPE_SCREEN_OFF => ScreenOff.getClassSchema
      case TYPE_SCREEN_UNLOCK => ScreenUnlock.getClassSchema
      case TYPE_SMS_RECEIVED => SmsReceived.getClassSchema
      case TYPE_SMS_SENT => SmsSent.getClassSchema
      case TYPE_TIMEZONE => TimeZone.getClassSchema
      case TYPE_TRAFFIC_DATA => TrafficData.getClassSchema
      case TYPE_UNKNOWN => Unknown.getClassSchema
      case  a if a == TYPE_WINDOW_STATE_CHANGED || a == TYPE_WINDOW_STATE_CHANGED_BASIC =>
        WindowStateChanged.getClassSchema
      case TYPE_WHATSAPP_SENT => WhatsAppSent.getClassSchema
      case TYPE_WHATSAPP_RECEIVED => WhatsAppReceived.getClassSchema
      case TYPE_QUESTIONNAIRE => Questionnaire.getClassSchema
      case _ => Unknown.getClassSchema
    }
  }
}


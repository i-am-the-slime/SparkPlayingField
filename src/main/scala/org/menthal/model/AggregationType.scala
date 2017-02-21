package org.menthal.model

/**
 * Created by konrad on 23.01.15.
 */
object AggregationType {
  //SMS
  val SmsInCount = "sms_in_count"
  val SmsOutCount = "sms_out_count"
  val SmsTotalCount = "sms_total_count"
  val SmsInParticipants = "sms_in_participants"
  val SmsOutParticipants = "sms_out_participants"
  val SmsTotalParticipants = "sms_total_participants"
  val SmsInLength = "sms_incoming_length"
  val SmsOutLength = "sms_outgoing_length"
  //WhatsApp
  val WhatsAppInCount = "whats_app_in_count"
  val WhatsAppOutCount = "whats_app_out_count"
  val WhatsAppTotalCount = "whats_app_total_count"
  val WhatsAppInParticipants = "whats_app_in_participants"
  val WhatsAppOutParticipants = "whats_app_out_participants"
  val WhatsAppTotalParticipants = "whats_app_total_participants"
  val WhatsAppInLength = "whats_app_incoming_length"
  val WhatsAppOutLength = "whats_app_outgoing_length"
  //CALLS
  val CallInCount = "call_in_count"
  val CallMissCount = "call_miss_count"
  val CallOutCount = "call_out_count"
  val CallTotalCount = "call_total_count"
  val CallInParticipants = "call_in_participants"
  val CallOutParticipants = "call_out_participants"
  val CallTotalParticipants = "call_participants_count"
  val CallInDuration = "call_in_duration"
  val CallOutDuration = "call_out_duration"
  val CallTotalDuration = "call_total_duration"
  //Apps
  val AppTotalCountUnique = "app_total_count_unique"
  val AppTotalCount = "app_total_count"
  val AppTotalDuration = "app_total_duration"
  //Notifications
  val NotificationCount = "notification_count"
  //Screen on / off
  val ScreenUnlocksCount = "screen_unlocks_count"
  val ScreenOffCount = "screen_off_count"
  val ScreenOnCount = "screen_on_count"
  // Phone
  val PhoneShutdownsCount = "phone_shutdowns_count"
  val PhoneBootsCount = "phone_boots_count"
  //Summary
  val Summary = "summary"
  //Category Aggregations
  val CategoryTotalCountUnique = "category_total_count_unique"
  val CategoryTotalCount = "category_total_count"
  val CategoryTotalDuration = "category_total_duration"

}

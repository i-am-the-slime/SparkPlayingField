package org.menthal.aggregations.tools

import org.menthal.model.AggregationType._
import org.menthal.model.events.Summary

/**
 * Created by konrad on 23.01.15.
 */
object SummaryTransformers {
  def createSummary(userId: Long, time: Long, granularity: Long, m: Map[String, Long]): Summary = {
    Summary.newBuilder()
      .setUserId(userId)
      .setTime(time)
      .setGranularity(granularity)
      //SMS
      .setSmsInCount(m(SmsInCount))
      .setSmsOutCount(m(SmsOutCount))
      .setSmsTotalCount(m(SmsTotalCount))
      .setSmsInParticipants(m(SmsInParticipants))
      .setSmsOutParticipants(m(SmsOutParticipants))
      .setSmsTotalParticipants(m(SmsTotalParticipants))
      .setSmsInLength(m(SmsInLength))
      .setSmsOutLength(m(SmsOutLength))
      //Calls
      .setCallInCount(m(CallInCount))
      .setCallMissCount(m(CallMissCount))
      .setCallOutCount(m(CallOutCount))
      .setCallTotalCount(m(CallTotalCount))
      .setCallInParticipants(m(CallInParticipants))
      .setCallOutParticipants(m(CallOutParticipants))
      .setCallTotalParticipants(m(CallTotalParticipants))
      .setCallInDuration(m(CallInDuration))
      .setCallOutDuration(m(CallOutDuration))
      .setCallTotalDuration(m(CallTotalDuration))
      //Apps
      .setAppTotalCountUnique(m(AppTotalCountUnique))
      .setAppTotalCount(m(AppTotalCount))
      .setAppTotalDuration(m(AppTotalDuration))
      //Screen
      .setScreenUnlocksCount(m(ScreenUnlocksCount))
      .setScreenOffCount(m(ScreenOffCount))
      .setScreenOnCount(m(ScreenOnCount))
      //Phone
      .setPhoneShutdownsCount(m(PhoneShutdownsCount))
      .setPhoneBootsCount(m(PhoneBootsCount))
      .build()
  }

  def summaryValues(s: Summary): Map[String, Long] = {
    Map(
      (SmsInCount, s.getSmsInCount),
      (SmsOutCount, s.getSmsOutCount),
      (SmsTotalCount, s.getSmsTotalCount),
      (SmsInParticipants, s.getSmsInParticipants),
      (SmsOutParticipants, s.getSmsOutParticipants),
      (SmsTotalParticipants, s.getSmsTotalParticipants),
      (SmsInLength, s.getSmsInLength),
      (SmsOutLength, s.getSmsOutLength),
      //Calls
      (CallInCount, s.getCallInCount),
      (CallMissCount, s.getCallMissCount),
      (CallOutCount, s.getCallOutCount),
      (CallTotalCount, s.getCallTotalCount),
      (CallInParticipants, s.getCallInParticipants),
      (CallOutParticipants, s.getCallOutParticipants),
      (CallTotalParticipants, s.getCallTotalParticipants),
      (CallInDuration, s.getCallInDuration),
      (CallOutDuration, s.getCallOutDuration),
      (CallTotalDuration, s.getCallTotalDuration),
      //Apps
      (AppTotalCountUnique, s.getAppTotalCountUnique),
      (AppTotalCount, s.getAppTotalCount),
      (AppTotalDuration, s.getAppTotalDuration),
      //Screen
      (ScreenUnlocksCount, s.getScreenUnlocksCount),
      (ScreenOffCount, s.getScreenOffCount),
      (ScreenOnCount, s.getScreenOnCount),
      //Phone
      (PhoneShutdownsCount, s.getPhoneShutdownsCount),
      (PhoneBootsCount, s.getPhoneBootsCount),
      (SmsInCount, s.getSmsInCount))
  }
}

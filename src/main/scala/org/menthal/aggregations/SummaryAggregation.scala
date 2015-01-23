package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.Granularity.TimePeriod
import org.menthal.model.events.{Summary, CCAggregationEntry}
import org.menthal.model.events.Implicits._

/**
 * Created by konrad on 22.01.15.
 */
object SummaryAggregation {

  type PerUserBucketsRDD[K, V] = RDD[(((Long, DateTime, K), V))]

  object Keys {
    //SMS
    val SmsInCount = "sms_in_count"
    val SmsOutCount = "sms_out_count"
    val SmsTotalCount = "sms_total_count"
    val SmsInParticipants = "sms_in_participants"
    val SmsOutParticipants = "sms_out_participants"
    val SmsTotalParticipants = "sms_total_participants"
    val SmsInLength = "sms_incoming_length"
    val SmsOutLength = "sms_outgoing_length"
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
    //Screen on / off
    val ScreenUnlocksCount = "screen_unlocks_count"
    val ScreenOffCount = "screen_off_count"
    val ScreenOnCount = "screen_on_count"
    // Phone
    val PhoneShutdownsCount = "phone_shutdowns_count"
    val PhoneBootsCount = "phone_boots_count"
  }
  //    ('sms_in_count' , ('A', ['INCOMING_MSG_COUNT'], sum_vals)),
  //    ('sms_out_count' , ('A', ['OUTGOING_MSG_COUNT'], sum_vals)),
  //    ('sms_total_count' , ('S', ['sms_out_count', 'sms_in_count'], id)),
  //   ('sms_in_participants' , ('A', ['INCOMING_MSG_COUNT'], count_participants)),
  //  ('sms_out_participants' , ('A', ['OUTGOING_MSG_COUNT'], count_participants)),
  //  ('sms_participants_count' , ('A', ['INCOMING_MSG_COUNT', 'OUTGOING_MSG_COUNT'], get_data, dict_sum, {}, len)),
  //  ('sms_in_length' , ('A', ['INCOMING_MSG_LENGTH'], sum_vals)),
  //  ('sms_out_length' , ('A', ['OUTGOING_MSG_LENGTH'], sum_vals)),
  //  ('call_in_count' , ('A', 'INCOMING_CALL_COUNT', sum_vals)),
  //  ('call_out_count' , ('A', ['OUTGOING_CALL_COUNT'], sum_vals)),
  //  ('call_miss_count' , ('A', ['MISSED_CALL_COUNT'], sum_vals)),
  //  ('call_total_count' , ('S', ['call_out_count', 'call_in_count', 'call_miss_count'], id)),
  //  ('call_in_participants' , ('A', 'INCOMING_CALL_COUNT', count_participants)),
  //  ('call_out_participants' , ('A', ['OUTGOING_CALL_COUNT'], count_participants)),
  //  ('call_participants_count' , ('A', ['INCOMING_CALL_COUNT', 'OUTGOING_CALL_COUNT'], get_data, dict_sum, {}, len)),
  //  ('call_in_duration' , ('A', 'INCOMING_CALL_DURATION', sum_vals)),
  //  ('call_out_duration' , ('A', ['OUTGOING_CALL_DURATION'], sum_vals)),
  //  ('call_total_duration' , ('S', ['call_out_duration', 'call_in_duration'], id)),
  //  ('app_total_count_unique' , ('A', ['APP_USAGE'], count_participants)),
  //  ('app_total_count' , ('A', ['APP_STARTS'], sum_vals)),
  //  ('app_total_duration' , ('A', ['APP_USAGE'], sum_vals)),
  //  ('screen_unlocks_count' , ('A', ['SCREEN_LOCK'], lambda a : get_val(a, 'screen_unlocks_count'))),
  //  ('screen_off_count' , ('A', ['SCREEN_LOCK'], lambda a: get_val(a, 'screen_off_count'))),
  //  ('screen_on_count' , ('A', ['SCREEN_LOCK'], lambda a: get_val(a, 'screen_on_count'))),
  //  ('phone_shutdowns_count' , ('A', ['PHONE_BOOT'], lambda a: get_val(a, 'phone_shutdowns_count'))),
  //  ('phone_boots_count' , ('A', ['PHONE_BOOT'], lambda a: get_val(a, 'phone_boots_count'))),



  def createSummary(userId: Long, time: Long, granularity: Long, m: Map[String, Long]): Summary = {
    //TODO: SOME ARE WRONG
    Summary.newBuilder()
      .setUserId(userId)
      .setTime(time)
      .setGranularity(granularity)
      .setSmsInCount(m(Keys.SmsInCount))
      .setSmsOutCount(m(Keys.SmsOutCount))
      .setSmsTotalCount(m(Keys.SmsTotalCount))
      .setSmsInParticipants(m(Keys.SmsInParticipants))
      .setSmsOutParticipants(m(Keys.SmsOutParticipants))
      .setSmsTotalParticipants(m(Keys.SmsTotalParticipants))
      .setSmsInLength(m(Keys.SmsInLength))
      .setSmsOutLength(m(Keys.SmsOutLength))
      .setCallInCount(m(Keys.CallInCount))
      .setCallMissCount(m(Keys.CallMissCount))
      .setCallOutCount(m(Keys.CallOutCount))
      .setCallTotalCount(m(Keys.CallTotalCount))
      .setCallInParticipants(m(Keys.CallInParticipants))
      .setCallOutParticipants(m(Keys.CallOutParticipants))
      .setCallTotalParticipants(m(Keys.CallTotalParticipants))
      .setCallInDuration(m(Keys.CallInDuration))
      .setCallOutDuration(m(Keys.CallOutDuration))
      .setCallTotalDuration(m(Keys.CallTotalDuration))
      .setAppTotalCountUnique(m(Keys.AppTotalCountUnique))
      .setAppTotalCount(m(Keys.AppTotalCount))
      .setAppTotalDuration(m(Keys.AppTotalDuration))
      .setScreenUnlocksCount(m(Keys.ScreenUnlocksCount))
      .setScreenOffCount(m(Keys.ScreenOffCount))
      .setScreenOnCount(m(Keys.ScreenOnCount))
      .setPhoneShutdownsCount(m(Keys.PhoneShutdownsCount))
      .setPhoneBootsCount(m(Keys.PhoneBootsCount))
      .build()
  }


  def aggregate(sc: SparkContext, datadir: String, granularity: TimePeriod): RDD[Summary] = {

    type AggTuple = ((Long, Long, Long), Long)
    def transformAggregationsInParquet(fn: RDD[CCAggregationEntry] => RDD[AggTuple])
                                      (inputAggrNames: List[String], name: String, granularity: TimePeriod): RDD[CCAggregationEntry] = {
      val inputRDDs = inputAggrNames.map(n => ParquetIO.readAggrType(sc, datadir, n, granularity).map(toCCAggregationEntry))
      val inputRDD = inputRDDs.reduce(_ ++ _)
      fn(inputRDD).map { case ((u, t, g), v) => CCAggregationEntry(u, t, g, name, v)}

    }
    def sumKeys(input: RDD[CCAggregationEntry]):RDD[AggTuple] = {
      input.map(e => ((e.userId, e.time, e.granularity), e.value)).reduceByKey(_ + _)
    }
    def countKeys(input: RDD[CCAggregationEntry]):RDD[AggTuple] = {
      input.map(e => ((e.userId, e.time, e.granularity), e.key)).distinct().mapValues(_ ⇒ 1L).reduceByKey(_ + _)
    }

    def sumValues = transformAggregationsInParquet(sumKeys) _

    def countDistinctKeys = transformAggregationsInParquet(countKeys) _

    val smsInCount:RDD[CCAggregationEntry] = sumValues(List("message_received_count"), Keys.SmsInCount, granularity)
    val smsOutCount:RDD[CCAggregationEntry] = sumValues(List("message_sent_count"), Keys.SmsOutCount, granularity)
    val smsTotalCount:RDD[CCAggregationEntry] = sumValues(List("message_received_count", "message_sent_count"), Keys.SmsTotalCount, granularity)
    val smsInParticipants:RDD[CCAggregationEntry] = countDistinctKeys(List("message_received_count"), Keys.SmsInParticipants, granularity)
    val smsOutParticipants:RDD[CCAggregationEntry] = countDistinctKeys(List("message_sent_count"), Keys.SmsOutParticipants, granularity)
    val smsTotalParticipants:RDD[CCAggregationEntry] = countDistinctKeys(List("message_received_count", "message_sent_count"), Keys.SmsTotalParticipants, granularity)
    val smsInLength:RDD[CCAggregationEntry] = sumValues(List("message_received_length"), Keys.SmsInLength, granularity)
    val smsOutLength:RDD[CCAggregationEntry] = sumValues(List("message_sent_length"), Keys.SmsOutLength, granularity)
    val callInCount:RDD[CCAggregationEntry] = sumValues(List("call_received_count"), Keys.CallInCount, granularity)
    val callMissCount:RDD[CCAggregationEntry] = sumValues(List("call_missed_count"), Keys.CallMissCount, granularity)
    val callOutCount:RDD[CCAggregationEntry] = sumValues(List("call_outgoing_count"), Keys.CallOutCount, granularity)
    val callTotalCount:RDD[CCAggregationEntry] = sumValues(List("call_received_count", "call_outgoing_count"), Keys.CallTotalCount, granularity)
    val callInParticipants:RDD[CCAggregationEntry] = countDistinctKeys(List("call_received_count"), Keys.CallInParticipants, granularity)
    val callOutParticipants:RDD[CCAggregationEntry] = countDistinctKeys(List("call_outgoing_count"), Keys.CallOutParticipants, granularity)
    val callTotalParticipants:RDD[CCAggregationEntry] = countDistinctKeys(List("call_received_count", "call_outgoing_count"), Keys.CallTotalParticipants, granularity)
    val callInDuration:RDD[CCAggregationEntry] = sumValues(List("call_received_duration"), Keys.CallInDuration, granularity)
    val callOutDuration:RDD[CCAggregationEntry] = sumValues(List("call_outgoing_duration"), Keys.CallOutDuration, granularity)
    val callTotalDuration:RDD[CCAggregationEntry] = sumValues(List("call_received_duration", "call_outgoing_duration"), Keys.CallTotalDuration, granularity)
    val appTotalCountUnique:RDD[CCAggregationEntry] = countDistinctKeys(List("app_starts"), Keys.AppTotalCountUnique, granularity)
    val appTotalCount:RDD[CCAggregationEntry] = sumValues(List("app_starts"), Keys.AppTotalCount, granularity)
    val appTotalDuration:RDD[CCAggregationEntry] = sumValues(List("app_usage"), Keys.AppTotalDuration, granularity)
    val screenUnlocksCount:RDD[CCAggregationEntry] = sumValues(List("screen_unlocks_count"), Keys.ScreenUnlocksCount, granularity)
    val screenOffCount:RDD[CCAggregationEntry] = sumValues(List("screen_off_count"), Keys.ScreenOffCount, granularity)
    val screenOnCount:RDD[CCAggregationEntry] = sumValues(List("screen_on_count"), Keys.ScreenOnCount, granularity)
    val phoneShutdownsCount:RDD[CCAggregationEntry] = sumValues(List("phone_off_count"), Keys.PhoneShutdownsCount, granularity)
    val phoneBootsCount:RDD[CCAggregationEntry] = sumValues(List("phone_on_count"), Keys.PhoneBootsCount, granularity)

    //TODO finish with all Aggregations
    val all = callInParticipants ++ callInCount
    all.groupBy(a => (a.userId, a.time, a.granularity))
      .mapValues(_.map(e => (e.key, e.value)).toMap.withDefaultValue(0L))
      .map { case ((u, t, g), m) => createSummary(u, t, g, m)}
  }
}

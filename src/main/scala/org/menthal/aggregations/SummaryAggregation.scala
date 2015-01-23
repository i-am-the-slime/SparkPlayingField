package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.Granularity.TimePeriod
import org.menthal.model.events.CCAggregationEntry
import org.menthal.model.events.Implicits._

/**
 * Created by konrad on 22.01.15.
 */
object SummaryAggregation {

  type PerUserBucketsRDD[K, V] = RDD[(((Long, DateTime, K), V))]

  case class CCSummaryAggregation(userId: Long, time: Long, granularity: Long,
                                  callInCount: Long, callInDuration: Long, callInParticipantsCount: Long,
                                  callOutCount: Long, callOutDuration: Long, callOutParticipantsCount: Long,
                                  callTotalCount: Long, callTotalDuration: Long, callTotalParticipantsCount: Long)

  //TODO move to Models - write Avro schema

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


  def createSummary(userId: Long, time: Long, granularity: Long, m: Map[String, Long]): CCSummaryAggregation = {
    CCSummaryAggregation(
      userId = userId,
      time = time,
      granularity = granularity,
      callInCount = m("callInCount"),
      callInParticipantsCount = m("callInCount"),
      callInDuration = m("callInDuration"),
      callOutCount = m("callOutCount"),
      callOutParticipantsCount = m("callOutCount"),
      callOutDuration = m("callInDuration"),
      callTotalCount = m("callInCount") + m("callOutCount"),
      callTotalDuration = m("callInCount") + m("callOutCount"),
      callTotalParticipantsCount = m("callTotalParticipant")
      //FInish to new Model
    )
  }


  def aggregate(sc: SparkContext, datadir: String, granularity: TimePeriod): RDD[CCSummaryAggregation] = {

    def transformAggregationsInParquet(fn: Long => Long)
                                      (inputAggrNames: List[String], name: String, granularity: TimePeriod): RDD[CCAggregationEntry] = {
      val inputRDDs = inputAggrNames.map(n => ParquetIO.readAggrType(sc, datadir, n, granularity).map(toCCAggregationEntry))
      val inputRDD = inputRDDs.reduce(_ ++ _)
      inputRDD.map(e => ((e.userId, e.time, e.granularity), fn(e.value)))
        .reduceByKey(_ + _)
        .map { case ((u, t, g), v) => CCAggregationEntry(u, t, g, name, v)}
    }

    def sumValues = transformAggregationsInParquet(identity) _

    def countKeys = transformAggregationsInParquet(_ => 1) _
    //Doesn't realy work with same key appearing multiple times - fix

    val callInCount: RDD[CCAggregationEntry] = sumValues(List("callReceived"), "callInCount", granularity)
    val callInParticipants: RDD[CCAggregationEntry] = countKeys(List("callReceived"), "callInParticipants", granularity)
    val callOutParticipants: RDD[CCAggregationEntry] = countKeys(List("callOutgoing"), "callInParticipants", granularity)
    val callTotalParticipants: RDD[CCAggregationEntry] = countKeys(List("callReceived", "callReceived"), "callInParticipants", granularity)
    //TODO finish with all Aggregations
    val all = callInParticipants ++ callInCount
    all.groupBy(a => (a.userId, a.time, a.granularity))
      .mapValues(_.map(e => (e.key, e.value)).toMap.withDefaultValue(0L))
      .map { case ((u, t, g), m) => createSummary(u, t, g, m)}
  }
}

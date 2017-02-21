package org.menthal.aggregations

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, Dataset}
import org.menthal.spark.SparkHelper.getSparkContext
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.EventType._
import org.menthal.model.Granularity
import org.menthal.model.Granularity._
import org.menthal.model.AggregationType
import org.menthal.model.events._
import org.menthal.model.events.Implicits._
import org.menthal.aggregations.tools.{GeneralAggregators, AggrSpec}
import org.menthal.aggregations.tools.AggrSpec._
import org.menthal.aggregations.tools.GeneralAggregators.MenthalDatasetAggregator

/**
 * Created by mark on 09.01.15.
 */
object GeneralAggregations {

  val name: String = "GeneralAggregations"

  def main(args: Array[String]) {
    val (master, datadir) = args match {
      case Array(m, d) => (m,d)
      case _ =>
        val errorMessage = "First argument is master, second input/output path"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, name)
    val sqlContext = SQLContext.getOrCreate(sc)
    aggregate(sqlContext, datadir)
    //fixFromHourly(sc, datadir)
    sc.stop()
  }

  private def aggregateAllSpecs(specs: List[AggrSpec[_ <: SpecificRecord]], granularities: GranularityForest)
                       (implicit sc:SparkContext, datadir:String) = {

    for {
      AggrSpec(eventType, converter, aggrs) ← specs
      events = ParquetIO.readEventType(sc, datadir, eventType).map(converter)
      (aggregator, aggrName) ← aggrs
      granularityTree ← granularities
      parquetAggregator = GeneralAggregators.aggregateToParquetForGranularity(aggrName, aggregator, eventType) _
    } granularityTree.traverseTree(events)(parquetAggregator)
  }

  private def aggregateFromSpecs(implicit sc: SparkContext, datadir: String): Unit = {
    aggregateAllSpecs(suite, Granularity.fullGranularitiesForest)
  }

  private def fixFromHourly(implicit sc: SparkContext, datadir: String): Unit = {
    aggregateAllSpecs(suite, Granularity.granularityForestFromDaily)
  }


  private val suite:List[AggrSpec[_ <: SpecificRecord]] = List(
    //Apps
    AggrSpec(TYPE_APP_SESSION, toCCAppSession _, countAndDuration(AggregationType.AppTotalDuration, AggregationType.AppTotalCount)),
    //Calls
    AggrSpec(TYPE_CALL_MISSED, toCCCallMissed _, count(AggregationType.CallMissCount)),
    AggrSpec(TYPE_CALL_OUTGOING, toCCCallOutgoing _, countAndDuration(AggregationType.CallOutCount, AggregationType.CallOutDuration)),
    AggrSpec(TYPE_CALL_RECEIVED, toCCCallReceived _, countAndDuration(AggregationType.CallInCount, AggregationType.CallInDuration)),
    //Notifications
    AggrSpec(TYPE_NOTIFICATION_STATE_CHANGED, toCCNotificationStateChanged _, count(AggregationType.NotificationCount)),
    //Screen
    AggrSpec(TYPE_SCREEN_OFF, toCCScreenOff _, count(AggregationType.ScreenOffCount)),
    AggrSpec(TYPE_SCREEN_ON, toCCScreenOn _, count(AggregationType.ScreenOnCount)),
    AggrSpec(TYPE_SCREEN_UNLOCK, toCCScreenUnlock _, count(AggregationType.ScreenUnlocksCount)),
    //SMS
    AggrSpec(TYPE_SMS_RECEIVED, toCCSmsReceived _, countAndLength(AggregationType.SmsInCount, AggregationType.SmsInLength)),
    AggrSpec(TYPE_SMS_SENT, toCCSmsSent _, countAndLength(AggregationType.SmsOutCount, AggregationType.SmsOutLength)),
    //Phone
    AggrSpec(TYPE_PHONE_SHUTDOWN, toCCPhoneShutdown _, count(AggregationType.PhoneShutdownsCount)),
    AggrSpec(TYPE_PHONE_BOOT, toCCPhoneBoot _, count(AggregationType.PhoneBootsCount)),
    //WhatsApp
    AggrSpec(TYPE_WHATSAPP_RECEIVED, toCCWhatsAppReceived _, countAndLength(AggregationType.WhatsAppInCount, AggregationType.WhatsAppInLength)),
    AggrSpec(TYPE_WHATSAPP_SENT, toCCWhatsAppSent _, countAndLength(AggregationType.WhatsAppOutCount, AggregationType.WhatsAppOutLength))
  )

  def aggregate(implicit sqlContext: SQLContext, datadir: String) = {
    import sqlContext.implicits._

    val appSessions = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_APP_SESSION).as[CCAppSession]
    countSumAndCountDistinctKeysAggrs(appSessions,
      AggregationType.AppTotalCount, AggregationType.AppTotalDuration, AggregationType.AppTotalCountUnique)
//
//    val callsMissed = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_CALL_MISSED).as[CCCallMissed]
//    countAggrs(callsMissed, AggregationType.CallMissCount)
//
//    val callsOutgoing = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_CALL_OUTGOING).as[CCCallOutgoing]
//    countSumAndCountDistinctKeysAggrs(callsOutgoing,
//      AggregationType.CallOutCount, AggregationType.CallOutDuration, AggregationType.CallOutParticipants)
//
//    val callsReceived = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_CALL_RECEIVED).as[CCCallReceived]
//    countSumAndCountDistinctKeysAggrs(callsReceived,
//      AggregationType.CallInCount, AggregationType.CallInDuration, AggregationType.CallInParticipants)
//
//
//    val notifications = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_NOTIFICATION_STATE_CHANGED).as[CCNotificationStateChanged]
//    countAggrs(notifications, AggregationType.NotificationCount)
//
//    val screenOffs = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_SCREEN_OFF).as[CCScreenOff]
//    countAggrs(screenOffs, AggregationType.ScreenOffCount)
//
//    val screenOns = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_SCREEN_ON).as[CCScreenOn]
//    countAggrs(screenOns, AggregationType.ScreenOnCount)
//
//    val screenUnlocks = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_SCREEN_UNLOCK).as[CCScreenUnlock]
//    countAggrs(screenUnlocks, AggregationType.ScreenUnlocksCount)
//
//    val smsesReceived = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_SMS_RECEIVED).as[CCSmsReceived]
//    countSumAndCountDistinctKeysAggrs(smsesReceived,
//      AggregationType.SmsInCount, AggregationType.SmsInLength, AggregationType.SmsInParticipants)
//
//    val smsesSent = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_SMS_SENT).as[CCSmsSent]
//    countSumAndCountDistinctKeysAggrs(smsesSent,
//      AggregationType.SmsOutCount, AggregationType.SmsOutLength, AggregationType.SmsOutParticipants)
//
//    val phoneShutdowns = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_PHONE_SHUTDOWN).as[CCPhoneShutdown]
//    countAggrs(phoneShutdowns, AggregationType.PhoneShutdownsCount)
//
//    val phoneBoots = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_PHONE_BOOT).as[CCPhoneBoot]
//    countAggrs(phoneBoots, AggregationType.PhoneBootsCount)
//
//    val whatsappReceived = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_WHATSAPP_RECEIVED).as[CCWhatsAppReceived]
//    countSumAndCountDistinctKeysAggrs(whatsappReceived,
//      AggregationType.WhatsAppInCount, AggregationType.WhatsAppInLength, AggregationType.WhatsAppInParticipants)
//
//    val whatsappSent = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_WHATSAPP_SENT).as[CCWhatsAppSent]
//    countSumAndCountDistinctKeysAggrs(whatsappSent,
//      AggregationType.WhatsAppOutCount, AggregationType.WhatsAppOutLength, AggregationType.WhatsAppOutParticipants)

  }

  def aggregateSingleDatasetSpec[T <:MenthalEvent](events: Dataset[T],
                                                   aggregators: (String, MenthalDatasetAggregator[T], Boolean)*)
                                                  (implicit sqlContext: SQLContext, datadir: String) = {
     events.cache()
     for ((name, aggregator, additive) <- aggregators)
       GeneralAggregators.aggregateToParquet(name, aggregator,events, additive)
     events.unpersist()
  }

  def countAggrs[T<:MenthalEvent](events:Dataset[T], countAggregationName: String)
                                   (implicit sqlContext: SQLContext, datadir: String) = {
    aggregateSingleDatasetSpec(events,
    (countAggregationName, GeneralAggregators.countEvents[T] _, true))
  }

  def countAndSumAggrs[T<:MenthalEvent](events:Dataset[T], countAggregationName: String, sumAggregationName: String)
                                      (implicit sqlContext: SQLContext, datadir: String) = {
    aggregateSingleDatasetSpec(events,
    (countAggregationName, GeneralAggregators.countEvents[T] _, true),
    (sumAggregationName, GeneralAggregators.sumEvents[T] _, true))
  }

  def countSumAndCountDistinctKeysAggrs[T<:MenthalEvent](events:Dataset[T], countAggregationName: String,
                                        sumAggregationName: String, countDistinctKeysAggregationName: String)
                                       (implicit sqlContext: SQLContext, datadir: String) = {
    aggregateSingleDatasetSpec(events,
      (countAggregationName, GeneralAggregators.countEvents[T] _, true),
      (sumAggregationName, GeneralAggregators.sumEvents[T] _, true),
      (countDistinctKeysAggregationName, GeneralAggregators.countKeys[T] _, false))
  }







}

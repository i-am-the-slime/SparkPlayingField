package org.menthal.aggregations

import org.apache.avro.specific.SpecificRecord
import org.menthal.model.EventType._
import org.menthal.model.events.Implicits._
import org.apache.spark.SparkContext
import org.menthal.aggregations.tools.AggrSpec
import org.menthal.model.Granularity
import AggrSpec._
import org.menthal.model.AggregationType
import org.menthal.spark.SparkHelper.getSparkContext

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
    aggregate(sc, datadir)
    //fixFromHourly(sc, datadir)
    sc.stop()
  }

  def aggregate(sc: SparkContext, datadir: String): Unit = {
    AggrSpec.aggregate(sc, datadir, suite, Granularity.fullGranularitiesForest)
  }

  def fixFromHourly(sc: SparkContext, datadir: String): Unit = {
    AggrSpec.aggregate(sc, datadir, suite, Granularity.granularityForestFromDaily)
  }


  val suite:List[AggrSpec[_ <: SpecificRecord]] = List(
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
    AggrSpec(TYPE_PHONE_BOOT, toCCPhoneBoot _, count(AggregationType.PhoneBootsCount))
    //WhatsApp
    //AggrSpec(TYPE_WHATSAPP_RECEIVED, toCCWhatsAppReceived _, countAndLength(AggregationType.WhatsAppInCount, AggregationType.WhatsAppInLength)),
    //AggrSpec(TYPE_WHATSAPP_SENT, toCCWhatsAppSent _, countAndLength(AggregationType.WhatsAppOutCount, AggregationType.WhatsAppOutLength))
  )

}

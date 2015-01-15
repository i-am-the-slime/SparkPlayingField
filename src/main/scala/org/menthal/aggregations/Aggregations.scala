package org.menthal.aggregations

import org.apache.avro.specific.SpecificRecord
import org.menthal.model.EventType._
import org.menthal.model.Granularity.TimePeriod
import org.menthal.model.events.Implicits._
import org.apache.spark.SparkContext
import org.menthal.aggregations.tools.{Tree, Node, Leaf}
import org.menthal.model.Granularity
import org.menthal.aggregations.AggrSpec._

/**
 * Created by mark on 09.01.15.
 */
object Aggregations {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: Aggregations <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Aggregations", System.getenv("SPARK_HOME"))
    val datadir = args(1)


    AggrSpec.aggregate(sc, datadir, suite, granularitiesForest)
    sc.stop()
  }

  val granularitiesForest:List[Tree[TimePeriod]] = List(Leaf(Granularity.Hourly),
    Node(Granularity.Daily, List(
      Node(Granularity.Monthly, List(
        Leaf(Granularity.Yearly))),
      Leaf(Granularity.Weekly))))

  val suite:List[AggrSpec[_ <: SpecificRecord]] = List(
    AggrSpec(TYPE_APP_SESSION, toCCAppSession, agDuration("app", "usage"), agCount("app", "starts")),
    AggrSpec(TYPE_CALL_MISSED, toCCCallMissed _, agCount("call_missed")),
    AggrSpec(TYPE_CALL_OUTGOING, toCCCallOutgoing _, agCountAndDuration("call_outgoing")),
    AggrSpec(TYPE_CALL_RECEIVED, toCCCallReceived _, agCountAndDuration("call_received")),
    AggrSpec(TYPE_NOTIFICATION_STATE_CHANGED, toCCNotificationStateChanged _, agCount("notification")),
    AggrSpec(TYPE_SCREEN_OFF, toCCScreenOff _, agCount("screen_off")),
    AggrSpec(TYPE_SCREEN_ON, toCCScreenOn _, agCount("screen_on")),
    AggrSpec(TYPE_SCREEN_UNLOCK, toCCScreenUnlock _, agCount("screen_unlock")),
    AggrSpec(TYPE_SMS_RECEIVED, toCCSmsReceived _, agCountAndLength("message_received")),
    AggrSpec(TYPE_SMS_SENT, toCCSmsSent _, agCountAndLength("message_sent")),
    AggrSpec(TYPE_WHATSAPP_RECEIVED, toCCWhatsAppReceived _, agCountAndLength("whatsapp_received")),
    AggrSpec(TYPE_WHATSAPP_SENT, toCCWhatsAppSent _, agCountAndLength("whatsapp_sent"))
  )



}

package org.menthal.io

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.menthal.spark.SparkHelper
import org.menthal.io.parquet.ParquetIO
import org.menthal.io.postgres.PostgresDump
import org.menthal.model.events._

import scala.reflect.ClassTag

object PostgresDumpToParquet {
  def main(args: Array[String]) {
    val (master, dumpFile, outputFile) = args match {
      case Array(m, d, o) =>
        (m,d,o)
      case _ =>
        val errorMessage = "First argument is master, second input path, third argument is output path"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = SparkHelper.getSparkContext(master, "AppSessionsAggregation")
    parseFromDumpAndWriteToParquet(sc, dumpFile, outputFile)
    sc.stop()
  }

  val eventTypes: List[(String, Class[_ <: SpecificRecord], Schema)] = List(
    ("app_install", classOf[AppInstall], AppInstall.getClassSchema),
    ("app_removal", classOf[AppRemoval], AppRemoval.getClassSchema),
    ("call_missed", classOf[CallMissed], CallMissed.getClassSchema),
    ("call_outgoing", classOf[CallOutgoing], CallOutgoing.getClassSchema),
    ("call_received", classOf[CallReceived], CallReceived.getClassSchema),
    ("dreaming_stopped", classOf[DreamingStopped], DreamingStopped.getClassSchema),
    ("dreaming_started", classOf[DreamingStarted], DreamingStarted.getClassSchema),
    ("localisation", classOf[Localisation], Localisation.getClassSchema),
    ("mood", classOf[Mood], Mood.getClassSchema),
    ("phone_boot", classOf[PhoneBoot], PhoneBoot.getClassSchema),
    ("phone_shutdown", classOf[PhoneShutdown], PhoneShutdown.getClassSchema),
    ("questionnaire", classOf[Questionnaire], Questionnaire.getClassSchema),
    ("screen_off", classOf[ScreenOff], ScreenOff.getClassSchema),
    ("screen_on", classOf[ScreenOn], ScreenOn.getClassSchema),
    ("screen_unlock", classOf[ScreenUnlock], ScreenUnlock.getClassSchema),
    ("sms_received", classOf[SmsReceived], SmsReceived.getClassSchema),
    ("sms_sent", classOf[SmsSent], SmsSent.getClassSchema),
    ("time_zone", classOf[TimeZone], TimeZone.getClassSchema),
    ("traffic_data", classOf[TrafficData], TrafficData.getClassSchema),
    ("whatsapp_received", classOf[WhatsAppReceived], WhatsAppReceived.getClassSchema),
    ("whatsapp_sent", classOf[WhatsAppSent], WhatsAppSent.getClassSchema),
    ("window_state_changed", classOf[WindowStateChanged], WindowStateChanged.getClassSchema))

  def filterAndWriteToParquet[A <:SpecificRecord](sc:SparkContext, events: RDD[MenthalEvent], path: String, schema: Schema, klazz: Class[A])(implicit ct:ClassTag[A]) = {
    val filteredEvents = events.filter(_.isInstanceOf[A]).map(_.asInstanceOf[A])
    ParquetIO.write(sc, filteredEvents, path, schema)
  }

  def parseFromDumpAndWriteToParquet(sc:SparkContext, dumpDirPath:String, outputPath:String) = {
    val menthalEvents = PostgresDump.parseDumpFile(sc, dumpDirPath)
    eventTypes.foreach {
      case (path, klazz, schema) =>  filterAndWriteToParquet(sc, menthalEvents, path, schema, klazz)
    }
  }
}

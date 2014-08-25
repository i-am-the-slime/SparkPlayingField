package org.menthal

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.menthal.model.events._
import org.menthal.model.scalaevents.adapters.PostgresDump
import org.menthal.model.serialization.ParquetIO
import org.menthal.model.events.Implicits._

import scala.reflect.ClassTag

object PostgresDumpToParquet {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("First argument is master, second input path, third argument is output path")
    }
    else {
      val sc = new SparkContext(args(0),
        "PostgresDumpToParquet",
        System.getenv("SPARK_HOME"),
        Nil,
        Map(
          "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
          "spark.kryo.registrator" -> "org.menthal.model.serialization.MenthalKryoRegistrator",
          "spark.kryo.referenceTracking" -> "false")
      )
      val dumpFile = args(1)
      val outputFile = args(2)
      work(sc, dumpFile, outputFile)
      sc.stop()
    }
  }

  def work(sc:SparkContext, dumpFilePath:String, outputPath:String) = {
    val menthalEvents = for {
      line <- sc.textFile(dumpFilePath)
      event <- PostgresDump.tryToParseLineFromDump(line)
    } yield event.toAvro


    //TODO: Maybe make this more generic
//    val appInstall = menthalEvents.filter(_.isInstanceOf[AppInstall])
//    ParquetIO.write(sc, appInstall, outputPath + "/app_install")
//    val appRemoval = menthalEvents.filter(_.isInstanceOf[AppRemoval])
//    ParquetIO.write(sc, appRemoval, outputPath + "/app_removal")
//    val callMissed = menthalEvents.filter(_.isInstanceOf[CallMissed])
//    ParquetIO.write(sc, callMissed, outputPath + "/call_missed")
//    val callOutgoing = menthalEvents.filter(_.isInstanceOf[CallOutgoing])
//    ParquetIO.write(sc, callOutgoing, outputPath + "/call_outgoing")
//    val callReceived = menthalEvents.filter(_.isInstanceOf[CallReceived])
//    ParquetIO.write(sc, callReceived, outputPath + "/call_received")
    val dreamingStarted = menthalEvents.filter(_.isInstanceOf[DreamingStarted])
    ParquetIO.write(sc, dreamingStarted, outputPath + "/dreaming_started")
//    val dreamingStopped = menthalEvents.filter(_.isInstanceOf[DreamingStopped])
//    ParquetIO.write(sc, dreamingStopped, outputPath + "/dreaming_stopped")
//    val localisation = menthalEvents.filter(_.isInstanceOf[Localisation])
//    ParquetIO.write(sc, localisation, outputPath + "/localisation")
//    val mood = menthalEvents.filter(_.isInstanceOf[Mood])
//    ParquetIO.write(sc, mood, outputPath + "/mood")
//    val phoneBoot = menthalEvents.filter(_.isInstanceOf[PhoneBoot])
//    ParquetIO.write(sc, phoneBoot, outputPath + "/phone_boot")
//    val phoneShutdown = menthalEvents.filter(_.isInstanceOf[PhoneShutdown])
//    ParquetIO.write(sc, phoneShutdown, outputPath + "/phone_shutdown")
//    val questionnaire = menthalEvents.filter(_.isInstanceOf[Questionnaire])
//    ParquetIO.write(sc, questionnaire, outputPath + "/questionnaire")
//    val screenOff = menthalEvents.filter(_.isInstanceOf[ScreenOff])
//    ParquetIO.write(sc, screenOff, outputPath + "/screen_off")
//    val screenOn = menthalEvents.filter(_.isInstanceOf[ScreenOn])
//    ParquetIO.write(sc, screenOn, outputPath + "/screen_on")
//    val screenUnlock = menthalEvents.filter(_.isInstanceOf[ScreenUnlock])
//    ParquetIO.write(sc, screenUnlock, outputPath + "/screen_unlock")
//    val smsReceived = menthalEvents.filter(_.isInstanceOf[SmsReceived])
//    ParquetIO.write(sc, smsReceived, outputPath + "/sms_received")
//    val smsSent = menthalEvents.filter(_.isInstanceOf[SmsSent])
//    ParquetIO.write(sc, smsSent, outputPath + "/sms_sent")
//    val timeZone = menthalEvents.filter(_.isInstanceOf[TimeZone])
//    ParquetIO.write(sc, timeZone, outputPath + "/time_zone")
//    val trafficData = menthalEvents.filter(_.isInstanceOf[TrafficData])
//    ParquetIO.write(sc, trafficData, outputPath + "/traffic_data")
//    val whatsappReceived = menthalEvents.filter(_.isInstanceOf[WhatsAppReceived])
//    ParquetIO.write(sc, whatsappReceived, outputPath + "/whatsapp_received")
//    val whatsappSent = menthalEvents.filter(_.isInstanceOf[WhatsAppSent])
//    ParquetIO.write(sc, whatsappSent, outputPath + "/whatsapp_sent")
//    val windowStateChanged = menthalEvents.filter(_.isInstanceOf[WindowStateChanged])
//    ParquetIO.write(sc, windowStateChanged, outputPath + "/window_state_changed")
  }

}

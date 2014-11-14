package org.menthal.io

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.menthal.model.EventType._
import org.menthal.spark.SparkHelper
import org.menthal.io.parquet.ParquetIO
import org.menthal.io.postgres.PostgresDump
import org.menthal.model.events._


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

  val processedTypes = List(
  //TYPE_APP_SESSION_TEST,
  //TYPE_APP_UPDATE,
  //TYPE_ACCESSIBILITY_SERVICE_UPDATE,
  TYPE_WINDOW_STATE_CHANGED,
  TYPE_WINDOW_STATE_CHANGED_BASIC,
  TYPE_SMS_RECEIVED,
  TYPE_SMS_SENT,
  TYPE_CALL_RECEIVED,
  TYPE_CALL_OUTGOING,
  TYPE_CALL_MISSED,
  TYPE_SCREEN_ON,
  TYPE_SCREEN_OFF,
  TYPE_LOCALISATION,
  //TYPE_APP_LIST, TODO
  TYPE_APP_INSTALL,
  //TYPE_APP_REMOVAL, TODO
  TYPE_MOOD,
  TYPE_PHONE_BOOT,
  TYPE_PHONE_SHUTDOWN,
  TYPE_SCREEN_UNLOCK,
  TYPE_DREAMING_STARTED,
  TYPE_DREAMING_STOPPED,
  TYPE_WHATSAPP_SENT,
  TYPE_WHATSAPP_RECEIVED,
  //TYPE_DEVICE_FEATURES, TODO
  //TYPE_MENTHAL_APP_ACTION, TODO
  //TYPE_TIMEZONE, TODO
  // TYPE_TRAFFIC_DATA, TODO
  //TYPE_APP_SESSION, TODO
  TYPE_QUESTIONNAIRE)

  def parseFromDumpAndWriteToParquet(sc:SparkContext, dumpDirPath:String, outputPath:String) = {
    val menthalEvents = PostgresDump.parseDumpFile(sc, dumpDirPath)
    processedTypes.foreach {
      case (eventType) =>  ParquetIO.filterAndWriteToParquet(sc, menthalEvents, eventType, outputPath)
    }
  }
}

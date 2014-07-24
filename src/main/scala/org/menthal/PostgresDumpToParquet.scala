package org.menthal

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.menthal.model.events.{CCSmsReceived, SmsReceived}
import org.menthal.model.scalaevents.adapters.PostgresDump
import org.menthal.model.serialization.ParquetIO
import org.menthal.model.events.Implicits._

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
    val eventsDump = sc.textFile(dumpFilePath)
    val smsReceived = for {
      line <- eventsDump
      event <- PostgresDump.tryToParseLineFromDump(line)
      if event.isInstanceOf[CCSmsReceived]
    } yield toSmsReceived(event.asInstanceOf[CCSmsReceived])

    ParquetIO.write(sc, smsReceived, outputPath, SmsReceived.SCHEMA$)

  }
}

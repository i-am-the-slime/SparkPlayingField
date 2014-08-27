package org.menthal

import org.apache.spark.{Partitioner, SparkContext}
//import org.menthal.AppSessionMonoid._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.twitter.algebird.Operators._
import org.menthal.model.events.{AppSession, MenthalEvent}
import org.menthal.model.scalaevents.adapters.PostgresDump
import org.menthal.model.serialization.ParquetIO

object AppSessionAggregations {
  def main(args:Array[String]) {
    if (args.length != 3) {
      System.err.println("First argument is master, second input path, third argument is output path")
    }
    else {
      val sc = new SparkContext(args(0),
        "AppSessionAggregation",
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
    val events = for {
      line <- sc.textFile(dumpFilePath)
      event <- PostgresDump.tryToParseLineFromDump(line)
    } yield event
    val sessions = reduceToAppSessions(events)
    ParquetIO.write(sc, sessions, outputPath + "/app_sessions", AppSession.getClassSchema)
  }

  def reduceToAppSessions(events:RDD[MenthalEvent]):RDD[AppSession] = {
    val containers = for {
      event <- events if AppSessionContainer.handledEvents.contains(event.getClass)
      time = event.time
      user = event.userId
      container = AppSessionContainer(event)
    } yield ((time, user), container)

    val sortedAndGrouped = containers.sortByKey().map{case ((time,user), container) => (user,container)}
    val reducedContainers = sortedAndGrouped.reduceByKey( _ + _ )
    reducedContainers flatMap {case (user, container) => container.toAppSessions(user)}
  }
}

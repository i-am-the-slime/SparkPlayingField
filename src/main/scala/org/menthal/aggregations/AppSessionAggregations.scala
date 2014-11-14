package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.menthal.aggregations.tools.{AppSessionMonoid, AppSessionContainer}
import org.menthal.io.parquet.ParquetIO
import org.menthal.io.postgres.PostgresDump
import org.menthal.model.events.{AppSession, MenthalEvent}
import org.menthal.spark.SparkHelper._

object AppSessionAggregations {

  def main(args: Array[String]) {
    val (master, dumpFile, outputFile) = args match {
      case Array(m, d, o) =>
        (m,d,o)
      case _ =>
        val errorMessage = "First argument is master, second input path, third argument is output path"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, "AppSessionsAggregation")
    dumpToAppSessions(sc, dumpFile, outputFile)
    sc.stop()
  }

  def dumpToAppSessions(sc:SparkContext, dumpFilePath:String, outputPath:String) = {
    val events = PostgresDump.parseDumpFile(sc, dumpFilePath)
    val sessions = eventsToAppSessions(events)
    ParquetIO write(sc, sessions, outputPath + "/app_sessions", AppSession.getClassSchema)
  }

  def transformToAppSessionsContainer(events:Iterable[_ <: MenthalEvent]):AppSessionContainer = {
    val containers = for {
      event <- events if AppSessionContainer.handledEvents.contains(event.getClass)
      container = AppSessionContainer(event)
    } yield container
    containers.fold(AppSessionMonoid.zero)(AppSessionMonoid.plus) //Does it makes sense to use par? nope, monoid addition will be slower
  }

  def eventsToAppSessions(events: RDD[_ <: MenthalEvent]):RDD[AppSession] = {
    events.map { e => (e.userId, e)}
      .groupByKey()
      .flatMap {case (userId, evs) =>
        val sortedEvents = evs.toArray.sortBy(_.time)
        val container = transformToAppSessionsContainer(sortedEvents)
        container.toAppSessions(userId)
    }
  }

}

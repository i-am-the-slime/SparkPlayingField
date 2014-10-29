package org.menthal

import org.apache.spark.{Partitioner, SparkContext}
import org.menthal.AppSessionMonoid._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.twitter.algebird.Operators._
import org.menthal.model.events.{AppSession, MenthalEvent}
import org.menthal.model.scalaevents.adapters.PostgresDump._
import org.menthal.model.serialization.ParquetIO
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
    val events = parseDumpFile(sc, dumpFilePath)
    val sessions = eventsToAppSessions(events)
    ParquetIO write(sc, sessions, outputPath + "/app_sessions", AppSession.getClassSchema)
  }

  def transformToAppSessionsContainer(events:Iterable[MenthalEvent]):AppSessionContainer = {
    val containers = for {
      event <- events if AppSessionContainer.handledEvents.contains(event.getClass)
      container = AppSessionContainer(event)
    } yield container
    containers.fold(AppSessionMonoid.zero)(_ + _) //Does it makes sense to use par? nope, monoid addition will be slower
  }

  def eventsToAppSessions(events: RDD[MenthalEvent]):RDD[AppSession] = {
    events.map { e => (e.userId, e)}
      .groupByKey()
      .flatMap {case (userId, evs) =>
        val sortedEvents = evs.toArray.sortBy(_.time)
        val container = transformToAppSessionsContainer(sortedEvents)
        container.toAppSessions(userId)
    }
  }

}

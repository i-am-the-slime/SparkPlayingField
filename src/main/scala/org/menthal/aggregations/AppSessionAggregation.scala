package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.menthal.aggregations.tools.{AppSessionMonoid, AppSessionContainer}
import org.menthal.io.parquet.ParquetIO
import org.menthal.io.postgres.PostgresDump
import org.menthal.model.EventType
import org.menthal.model.events._
import org.menthal.model.events.Implicits._

import EventType._
import org.menthal.spark.SparkHelper._

object AppSessionAggregation {

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

  def aggregate(sc:SparkContext, datadir:String, outputPath:String) = {
    val sessions = parquetToAppSessions(sc, datadir)
    ParquetIO.writeEventType(sc, outputPath, TYPE_APP_SESSION, sessions)
  }

  def parquetToAppSessions(sc:SparkContext, datadir:String):RDD[AppSession] = {
    val screenOff:RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SCREEN_OFF).map(toCCScreenOff)
    val windowStateChanged:RDD[MenthalEvent]  = ParquetIO.readEventType(sc, datadir, TYPE_WINDOW_STATE_CHANGED).map(toCCWindowStateChanged)
    val screenUnlock:RDD[MenthalEvent] = ParquetIO.readEventType(sc, datadir, TYPE_SCREEN_UNLOCK).map(toCCScreenUnlock)
    val dreamingStarted:RDD[MenthalEvent]  = ParquetIO.readEventType(sc, datadir, TYPE_DREAMING_STARTED).map(toCCDreamingStarted)
    val processedEvents = screenOff ++ windowStateChanged ++ screenUnlock ++ dreamingStarted
    eventsToAppSessions(processedEvents)
  }

  def dumpToAppSessions(sc:SparkContext, dumpFilePath:String, outputPath:String) = {
    val events = PostgresDump.parseDumpFile(sc, dumpFilePath)
    val sessions = eventsToAppSessions(events)
    ParquetIO.writeEventType(sc, outputPath, TYPE_APP_SESSION, sessions)
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

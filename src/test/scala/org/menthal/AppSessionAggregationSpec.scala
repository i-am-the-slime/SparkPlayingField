package org.menthal

import org.menthal.model.events.{MenthalEvent, AppSession}
import org.menthal.model.scalaevents.adapters.PostgresDump
import org.scalatest._
import org.apache.spark.{SparkConf, SparkContext}
import org.menthal.AppSessionMonoid._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.twitter.algebird.Operators._
import org.menthal.model.events.{AppSession, MenthalEvent}
import org.menthal.model.scalaevents.adapters.PostgresDump
import org.menthal.model.serialization.ParquetIO

import scala.io.Source
import scala.reflect.io.File
import scala.util.Try

class AppSessionAggregationsSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  @transient var sc:SparkContext = _

  override def beforeEach(){
    ysc = SparkTestHelper.localSparkContext
  }

  override def afterEach() = {
    sc.stop()
    sc = null
  }

  def eventsToAppSessions(events: RDD[MenthalEvent]):RDD[AppSession] = ?


 def transformToAppSessionsContainer(events:Iterable[MenthalEvent]):AppSessionContainer = ?

  "The function transformToAppSessionsContainer" should "create AppSessionContainer from sorted MenthalEvents" in {

  }

  "The function eventsToAppSession" should
    "take an RDD of MenthalEvents and return RDD with AppSession based on them" in {
    val eventLines = Source.fromURL(getClass.getResource("/real_events.small")).getLines().toList
    val mockRDDs = sc.parallelize(eventLines, 4)
  }

  "The function dumpToAppSesiions" should "take an path to file and return another RDD of String" in {
    val eventLines = Source.fromURL(getClass.getResource("/real_events.small")).getLines().toList
    val mockRDDs = sc.parallelize(eventLines, 4)
    val events = mockRDDs.flatMap(line => PostgresDump.tryToParseLineFromDump(line))i

    val containers = for {
      event <- events if AppSessionContainer.handledEvents.contains(event.getClass)
      time = event.time
      user = event.userId
      container = AppSessionContainer(event)
    } yield ((user, time), container)

    info(s"${containers.count()}")
    val sortedAndGrouped = containers.sortByKey().map {case ((user, time), container) => (user,container)}
    val reducedContainers = sortedAndGrouped.foldByKey(AppSessionMonoid.zero)(_ + _)
    val result = reducedContainers flatMap {case (user, container) => container.toAppSessions(user)}
    val len = result.count()

    result.take(10).foreach(Writer.printSession)
    len should not be 0
  }
    object Writer{
      def printEvent(event: MenthalEvent) = info(s"Event $event")
      def printSession(session:AppSession) = info(s"User \nSessions $session")
    }

}

package org.menthal

import org.menthal.model.events._
import org.menthal.model.scalaevents.adapters.PostgresDump
import org.scalatest._
import org.apache.spark.{SparkConf, SparkContext}
import org.menthal.AppSessionMonoid._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.twitter.algebird.Operators._
import org.menthal.model.scalaevents.adapters.PostgresDump
import org.menthal.model.serialization.ParquetIO

import scala.collection.immutable.Queue
import scala.io.Source
import scala.reflect.io.File
import scala.util.Try

class AppSessionAggregationsSpec extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfter{

  val basePath = "src/test/resources/"

  @transient var sc:SparkContext = _

  override def beforeEach(){
    sc = SparkTestHelper.localSparkContext
  }

  override def afterEach() = {
    sc.stop()
    sc = null
  }



  "The function dumpToAppSessions" should "read a dump file and produce parquet output with app sessions" in {
    val outputPath = basePath+"appSessionsParquetted"
    val inputPath = basePath + "raw_events_with_wsc"
    Try(File(outputPath).deleteRecursively())
    AppSessionAggregations.dumpToAppSessions(sc, inputPath, outputPath)
    val result:Array[AppSession] = ParquetIO.read(outputPath + "/app_sessions", sc).collect()
    val correct = Array(
          new AppSession(22812L,  1390427044719L, 120000L, "com.whatsapp")
        , new AppSession(22812L, 1390427164719L, 69590949L, "pussycat.james.bond")
        , new AppSession(22812L, 1390521066820L, 127387L, "pussycat.james.bond")
        , new AppSession(22812L, 1390521194207L, 0L, "com.whatsapp")
    )
    result shouldBe correct
  }

//  "When given an input and output file" should "read the file and output the results" in {
//    PostgresDumpToParquet.parseFromDumpAndWriteToParquet(sc, inputPath , outputPath)
//    val result:RDD[SmsReceived] = ParquetIO.read(outputPath + "/sms_received", sc)
//    val someEvent = result.filter(smsRec => smsRec.getId == 191444L).take(1).toList(0)
//    val hash = "43bd5f4b6f51cb3a007fb2683ca64e7788a0fc02f84c52670e8086b491bcb85572ae8772b171910ee2cbce1f3fe27a7fa3634d0c97a8c5f925de9eb21b574179"
//    val expectedEvent = new SmsReceived(191444L, 1L, 1369369344990L, hash, 129)
//    someEvent shouldBe expectedEvent
//  }

  "The function eventsToAppSessions" should "turn sorted events for the same user into app sessions" in {
    val events = List(
      CCWindowStateChanged(0, 0, 1, "", "A", ""),
      CCWindowStateChanged(0, 0, 2, "", "B", ""), //User 0
      CCScreenOff(0, 0, 4)
    )
    val sparkEvents:RDD[MenthalEvent] = sc.parallelize(events)
    val result = AppSessionAggregations.eventsToAppSessions(sparkEvents).collect()
    val expected = Array(
      new AppSession(0L, 1L, 1L, "A"),
      new AppSession(0L, 2L, 2L, "B")
    )
    info(result.toString)
    result shouldBe expected
  }

  "The function eventsToAppSessions" should "turn UNsorted events for the same user into app sessions" in {
    val events = List(
      CCWindowStateChanged(1, 0, 2, "", "B", ""), //User 0
      CCWindowStateChanged(2, 0, 1, "", "A", ""),
      CCScreenOff(3, 0, 4)
    )
    val sparkEvents:RDD[MenthalEvent] = sc.parallelize(events)
    val result = AppSessionAggregations.eventsToAppSessions(sparkEvents).collect()
    val expected = Array(
      new AppSession(0L, 1L, 1L, "A"),
      new AppSession(0L, 2L, 2L, "B")
    )
    info(result.toString)
    result shouldBe expected
  }

  "The function eventsToAppSessions" should "turn unsorted events for two different users into app sessions" in {
    val events = List(
      CCWindowStateChanged(1, 0, 2, "", "B", ""), //User 0
      CCWindowStateChanged(2, 0, 1, "", "A", ""),
      CCWindowStateChanged(3, 1, 2, "", "B", ""), //User 1
      CCScreenOff(4, 0, 4),
      CCScreenOff(5, 1, 4)
    )
    val sparkEvents:RDD[MenthalEvent] = sc.parallelize(events)
    val result = AppSessionAggregations.eventsToAppSessions(sparkEvents).collect()
    val expected = Array(
      new AppSession(0L, 1L, 1L, "A"),
      new AppSession(0L, 2L, 2L, "B"),
      new AppSession(1L, 2L, 2L, "B")
    )
    result shouldBe expected
  }


  "The function transformToAppSessionsContainer" should "create AppSessionContainer from sorted MenthalEvents" in {
    val events = List(
    CCWindowStateChanged(1,0,  1, "", "A", ""),
    CCWindowStateChanged(2,0,  2, "", "B", ""),
    CCWindowStateChanged(3,0,  3, "", "C", ""),
    CCScreenOff(4,0,  4)
    )
    val result = AppSessionAggregations.transformToAppSessionsContainer(events)
    result shouldBe AppSessionContainer(
      Session(1, 2, Some("A")),
      Session(2, 3, Some("B")),
      Session(3, 4, Some("C")),
      Lock(4, Some("C")))
  }

  "The function eventsToAppSession" should
    "take an RDD of MenthalEvents and return RDD with AppSession based on them" in {
    val eventLines = Source.fromURL(getClass.getResource("/real_events.small")).getLines().toList
    val mockRDDs = sc.parallelize(eventLines, 4)
  }

  "The function dumpToAppSessions" should "take an path to file and return another RDD of String" in {
    val eventLines = Source.fromURL(getClass.getResource("/real_events.small")).getLines().toList

    val mockRDDs = sc.parallelize(eventLines, 4)
    val events = mockRDDs.flatMap(line => PostgresDump.tryToParseLineFromDump(line))

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

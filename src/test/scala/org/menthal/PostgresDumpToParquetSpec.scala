package org.menthal

import java.util.logging.{Level, Logger}

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.menthal.io.PostgresDumpToParquet
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.events._
import org.menthal.model.EventType._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FlatSpec, Matchers}

import scala.reflect.ClassTag
import scala.reflect.io.File
import scala.util.Try

class PostgresDumpToParquetSpec extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfter{
  //TODO fix localpaths in this file

  @transient var sc:SparkContext = _
  val basePath = "src/test/resources/"
  val inputPath = basePath + "real_events.small"
  val outputPath = basePath + "PostgresDumpToParquetSpecTestFile"

  override def beforeEach() {
    Try(File(outputPath).deleteRecursively())
    sc = SparkTestHelper.localSparkContext
  }

  override def afterEach() {
    Try(File(outputPath).deleteRecursively())
    sc.stop()
    sc = null
  }

  "When given an input and output file" should "read the file and output the results" in {
    PostgresDumpToParquet.parseFromDumpAndWriteToParquet(sc, inputPath , outputPath)
    val result:RDD[SmsReceived] = ParquetIO.readEventType(outputPath, TYPE_SMS_RECEIVED, sc)
    val someEvent = result.filter(smsRec => smsRec.getId == 191444L).take(1).toList(0)
    val hash = "43bd5f4b6f51cb3a007fb2683ca64e7788a0fc02f84c52670e8086b491bcb85572ae8772b171910ee2cbce1f3fe27a7fa3634d0c97a8c5f925de9eb21b574179"
    val expectedEvent = new SmsReceived(191444L, 1L, 1369369344990L, hash, 129)
    someEvent shouldBe expectedEvent
  }

  "When given an input and output file" should "read the file and output the results for SMS sent" in {
    PostgresDumpToParquet.parseFromDumpAndWriteToParquet(sc, inputPath , outputPath)
    val result:RDD[SmsSent] = ParquetIO.readEventType(outputPath, TYPE_SMS_SENT, sc)
    val someEvent = result.filter(smsRec => smsRec.getId == 192040L).take(1).toList(0)
    val hash = "676962b809fac8b6cb64113f6ee3bc594ca3e7b59cb35863c15dd69f668b7763131e0c9a7708f5d9201e1e64783ac6eeb14c36b382bf7da14042575c54230f46"
    val expectedEvent = new SmsSent(192040L, 1L, 1369649490619L, hash, 125)
    someEvent shouldBe expectedEvent
  }

  behavior of "Correct Parquet saving and loading from the dump file"

  it should "work for WindowStateChanged" in {
    type ET = WindowStateChanged
    val event = new WindowStateChanged(194430L, 1L, 1369864301015L, "System Android", "android", "")
    val typeNo = TYPE_WINDOW_STATE_CHANGED
    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for NotificationStateChanged" in {
    type ET = NotificationStateChanged
    val event = new NotificationStateChanged(191100L, 154L, 1369866730909L, "Google Search",
      "com.google.android.googlequicksearchbox", 2L)
    val typeNo = TYPE_NOTIFICATION_STATE_CHANGED

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for SMS Received" in {
    type ET = SmsReceived
    val event = new SmsReceived(194660L, 2L, 1369880442744L,
      "6fa882592487c294b76b86b315ac9276bbcb924b93af8e40f73fde9044c23850dd20fe25068b5ef9156480c9b7fe63ff67b25ba984331cc26fc2658bd2382e8d",
       135)
    val typeNo = TYPE_SMS_RECEIVED

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for SMS Sent" in {
    type ET = SmsSent
    val event = new SmsSent(192040L, 1L, 1369649490619L,
      "676962b809fac8b6cb64113f6ee3bc594ca3e7b59cb35863c15dd69f668b7763131e0c9a7708f5d9201e1e64783ac6eeb14c36b382bf7da14042575c54230f46",
      125)
    val typeNo = TYPE_SMS_SENT

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for Call Received" in {
    type ET = CallReceived
    val event = new CallReceived(192040L, 154L, 1369880442744L,
      "613a016635fc407c1b3a1b6122f2469aac080a0b3ab37110e0d92d2be5d84acda400260ffc7fc771b16af49588babd2f326eb61c5ad903a499693ba36a9e5651",
      1369854866723L, 64L)
    val typeNo = TYPE_CALL_RECEIVED

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }


  it should "work for Call Outgoing" in {
    type ET = CallOutgoing
    val event = new CallOutgoing(190829L, 154L, 1369853029838L,
      "613a016635fc407c1b3a1b6122f2469aac080a0b3ab37110e0d92d2be5d84acda400260ffc7fc771b16af49588babd2f326eb61c5ad903a499693ba36a9e5651",
      1369852973032L, 43L)
    val typeNo = TYPE_CALL_OUTGOING

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for Call Missed" in {
//    type ET = CallMissed
//    val event = new CallMissed(192040L, 1L, 1369880442744L,
//      "676962b809fac8b6cb64113f6ee3bc594ca3e7b59cb35863c15dd69f668b7763131e0c9a7708f5d9201e1e64783ac6eeb14c36b382bf7da14042575c54230f46",
//      125)
//    val typeNo = TYPE_CALL_MISSED
//
//    //Always the same
//    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
//    if (event != result) error("Expected:   " + event + "\nActual      " +result)
////    result shouldBe event
    "we need one example of this" shouldBe ""
  }

  it should "work for Screen On" in {
    type ET = ScreenOn
    val event = new ScreenOn(194670L, 2L, 1369903309675L)
    val typeNo = TYPE_SCREEN_ON

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for Screen Off" in {
    type ET = ScreenOff
    val event = new ScreenOff(190840L, 154L, 1369853566432L)
    val typeNo = TYPE_SCREEN_OFF

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for Screen Unlock" in {
    type ET = ScreenUnlock
    val event = new ScreenUnlock(194671L, 2L, 1369903310758L)
    val typeNo = TYPE_SCREEN_UNLOCK

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for Localisation (GPS)" in {
    type ET = Localisation
    val event = new Localisation(194570L, 154L, 1369890110233L, "gps", 10.0f, 7.12154483, 50.735962)
    val typeNo = TYPE_LOCALISATION

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for AppInstall" in {
    type ET = AppInstall
    val event = new AppInstall(195669L, 2L, 1369880442744L,"Nyx", "com.menthal.nyx")
    val typeNo = TYPE_APP_INSTALL

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for Mood" in {
    type ET = Mood
    val event = new Mood(192040L, 1L, 1369880442744L, 4.0f)
    val typeNo = TYPE_MOOD

    //Always the same
    val result = writeAndGet[ET](typeNo, filter = _.getId == event.getId )
    if (event != result) error("Expected:   " + event + "\nActual      " +result)
    result shouldBe event
  }

  it should "work for DreamingStarted" in {
    PostgresDumpToParquet.parseFromDumpAndWriteToParquet(sc, inputPath , outputPath)
    val result:RDD[DreamingStarted] = ParquetIO.readEventType(outputPath, TYPE_DREAMING_STARTED, sc)
    val someDreamingEvent = result.filter(dreamRec => dreamRec.getId == 1910171L).take(1).toList(0)
    val expectedDreamingEvent = new DreamingStarted(1910171L, 154L, 1369858030111L)
    someDreamingEvent shouldBe expectedDreamingEvent
  }

  it should "work for DreamingStopped" in {
    val eventType = TYPE_DREAMING_STOPPED
    val filter = (dreamRec:DreamingStopped) => dreamRec.getId == 1910172L
    val expectedDreamingEvent = new DreamingStopped(1910172L, 154L, 1369858030111L)
    writeAndGet[DreamingStopped](eventType, filter) shouldBe expectedDreamingEvent
  }

  it should "work for AppRemoval" in {
    val eventType = TYPE_APP_REMOVAL
    val filter = (ev:AppRemoval) => ev.getId == 3045489513L
    val expectedEvent = new AppRemoval(3045489513L, 8912L, 1415938023906L, "YouTube", "com.google.android.youtube")
    writeAndGet[AppRemoval](eventType, filter) shouldBe expectedEvent
  }

  it should "work for TrafficData" in {
    val eventType = TYPE_TRAFFIC_DATA
    val filter = (ev:TrafficData) => ev.getId == 2149587751L
    val expectedEvent =
      new TrafficData(2149587751L, 42535L, 1407583604729L, 1, "\"FRITZ!Box	6360	Cable\"", 0, 780327L, 102914L, 0L)
    writeAndGet[TrafficData](eventType, filter) shouldBe expectedEvent
  }

  def writeAndGet[A <: SpecificRecord](eventType: Int, filter: A => Boolean) (implicit ct : ClassTag[A]):A = {
    PostgresDumpToParquet.parseFromDumpAndWriteToParquet(sc, inputPath, outputPath)
    val result:RDD[A] = ParquetIO.readEventType[A](outputPath, eventType, sc)
    result.filter(filter).take(1).toList(0)
  }
}

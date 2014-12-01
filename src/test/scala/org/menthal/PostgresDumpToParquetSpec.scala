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

  "When given an input and output file" should "read the file and output the results for smss sent" in {
    PostgresDumpToParquet.parseFromDumpAndWriteToParquet(sc, inputPath , outputPath)
    val result:RDD[SmsSent] = ParquetIO.readEventType(outputPath, TYPE_SMS_SENT, sc)
    val someEvent = result.filter(smsRec => smsRec.getId == 192040L).take(1).toList(0)
    val hash = "676962b809fac8b6cb64113f6ee3bc594ca3e7b59cb35863c15dd69f668b7763131e0c9a7708f5d9201e1e64783ac6eeb14c36b382bf7da14042575c54230f46"
    val expectedEvent = new SmsSent(192040L, 1L, 1369649490619L, hash, 125)
    someEvent shouldBe expectedEvent
  }

  "When given an dump file " should "read the file and output the results" in {
    PostgresDumpToParquet.parseFromDumpAndWriteToParquet(sc, inputPath , outputPath)
    val result:RDD[DreamingStarted] = ParquetIO.readEventType(outputPath, TYPE_DREAMING_STARTED, sc)
    val someDreamingEvent = result.filter(dreamRec => dreamRec.getId == 1910171L).take(1).toList(0)
    val expectedDreamingEvent = new DreamingStarted(1910171L, 154L, 1369858030111L)
    someDreamingEvent shouldBe expectedDreamingEvent
  }

  "When given an dump file " should "read the Dreaming stopped events and write them to Parquet correctly" in {
    val eventType =TYPE_DREAMING_STOPPED
    val filter = (dreamRec:DreamingStopped) => dreamRec.getId == 1910172L
    val expectedDreamingEvent = new DreamingStopped(1910172L, 154L, 1369858030111L)
    parquetTest[DreamingStopped](eventType, filter) shouldBe expectedDreamingEvent
  }

  "When given an dump file " should "read the AppRemoval events and write them to Parquet correctly "  in {
    val eventType =TYPE_APP_REMOVAL
    val filter = (ev:AppRemoval) => ev.getId == 3045489513L
    val expectedEvent = new AppRemoval(3045489513L, 8912L, 1369858030111L, "YouTube", "com.google.android.youtube")
    parquetTest[AppRemoval](eventType, filter) shouldBe expectedEvent
  }

  "When given an dump file " should "read the TrafficData events and write them to Parquet correctly "  in {
    val eventType =TYPE_TRAFFIC_DATA
    val filter = (ev:TrafficData) => ev.getId == 2149587751L
    val expectedEvent = new TrafficData(2149587751L, 42535L, 1407583604729L, 1, "FRITZ!Box\t6360\tCable", 0, 780327L, 102914L, 0L)
    parquetTest[TrafficData](eventType, filter) shouldBe expectedEvent
  }




  def parquetTest[A <: SpecificRecord](eventType: Int, filter: A => Boolean)(implicit cd : ClassTag[A]):A = {
    PostgresDumpToParquet.parseFromDumpAndWriteToParquet(sc, inputPath, outputPath)
    val result:RDD[A] = ParquetIO.readEventType[A](outputPath, eventType, sc)
    info("\n\n" + result.count().toString +  "\n\n")
    result.filter(filter).take(1).toList(0)
  }



}

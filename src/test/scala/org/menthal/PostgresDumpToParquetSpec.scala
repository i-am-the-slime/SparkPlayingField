package org.menthal

import java.util.logging.{Level, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.menthal.model.events._
import org.menthal.model.serialization.ParquetIO
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.reflect.io.File
import scala.util.Try

class PostgresDumpToParquetSpec extends FlatSpec with Matchers with BeforeAndAfterEach{
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

//  "When given an input and output file" should "read the file and output the results" in {
  ignore should "read the file and output the results correctly" in {
    PostgresDumpToParquet.work(sc, inputPath , outputPath)
    val result:RDD[SmsReceived] = ParquetIO.read(outputPath + "/sms_received", sc)
    val someEvent = result.filter(smsRec => smsRec.getId == 191444L).take(1).toList(0)
    val hash = "43bd5f4b6f51cb3a007fb2683ca64e7788a0fc02f84c52670e8086b491bcb85572ae8772b171910ee2cbce1f3fe27a7fa3634d0c97a8c5f925de9eb21b574179"
    val expectedEvent = new SmsReceived(191444L, 1L, 1369369344990L, hash, 129)
    someEvent shouldBe expectedEvent
  }

//  "When given an input and output file" should "read the file and output the results for smss sent" in {
  ignore should "read the file and output the results for smss sent" in {
    PostgresDumpToParquet.work(sc, inputPath , outputPath)
    val result:RDD[SmsSent] = ParquetIO.read(outputPath + "/sms_sent", sc)
    val someEvent = result.filter(smsRec => smsRec.getId == 192040L).take(1).toList(0)
    val hash = "676962b809fac8b6cb64113f6ee3bc594ca3e7b59cb35863c15dd69f668b7763131e0c9a7708f5d9201e1e64783ac6eeb14c36b382bf7da14042575c54230f46"
    val expectedEvent = new SmsSent(192040L, 1L, 1369649490619L, hash, 125)
    someEvent shouldBe expectedEvent
  }

//  "When given an dreaming input and output file" should "read the file and output the results" in {
  ignore should "read the file and output the results" in {
    PostgresDumpToParquet.work(sc, inputPath , outputPath)
    val result:RDD[DreamingStarted] = ParquetIO.read(outputPath + "/dreaming_started", sc)
    val someDreamingEvent = result.filter(dreamRec => dreamRec.getId == 1910171L).take(1).toList(0)
    val expectedDreamingEvent = new DreamingStarted(1910171L, 154L, 1369858030111L)
    someDreamingEvent shouldBe expectedDreamingEvent
  }

//  "When given an dreaming stopped input and output file" should "read the file and output the results" in {
  ignore should "read the file and output the results, too" in {
    PostgresDumpToParquet.work(sc, inputPath , outputPath)
    val result:RDD[DreamingStopped] = ParquetIO.read(outputPath + "/dreaming_stopped", sc)
    val someDreamingEvent = result.filter(dreamRec => dreamRec.getId == 1910172L).take(1).toList(0)
    val expectedDreamingEvent = new DreamingStopped(1910172L, 154L, 1369858030111L)
    someDreamingEvent shouldBe expectedDreamingEvent
  }

  //"The big dump"
  ignore should "work" in {
    val path = "/Users/mark/temp/events_dump_24_01_13.sql"
    PostgresDumpToParquet.work(sc, path, path+"Dembowski")
  }

//  "When given a file with possibly wrong dreaming events" should "crash or not" in {
  ignore should "crash or not" in {
    val path = "/Users/mark/temp/dreamingstarted"
    PostgresDumpToParquet.work(sc, path, path+"OUT")
  }
}

package org.menthal

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.menthal.model.events.{CCSmsReceived, CallReceived, SmsReceived}
import org.menthal.model.serialization.ParquetIO
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.reflect.io.File
import scala.util.Try

class PostgresDumpToParquetSpec extends FlatSpec with Matchers with BeforeAndAfterEach{

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
    PostgresDumpToParquet.work(sc, inputPath , outputPath)
    val result:RDD[SmsReceived] = ParquetIO.read(outputPath, sc)
    val someEvent = result.filter(smsRec => smsRec.getId == 191444L).take(1).toList(0)
    val hash = "43bd5f4b6f51cb3a007fb2683ca64e7788a0fc02f84c52670e8086b491bcb85572ae8772b171910ee2cbce1f3fe27a7fa3634d0c97a8c5f925de9eb21b574179"
    val expectedEvent = new SmsReceived(191444L, 1L, 1369369344990L, hash, 129)
    someEvent shouldBe expectedEvent
  }
}

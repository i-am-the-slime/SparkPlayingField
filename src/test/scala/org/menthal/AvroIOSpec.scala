package org.menthal

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import org.apache.avro.Schema.Parser
import org.menthal.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

import scala.reflect.io.File
import org.menthal.model.events._
import org.apache.spark.SparkContext._

import parquet.avro.{AvroWriteSupport, AvroParquetOutputFormat}
import parquet.hadoop.ParquetOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.Schema

class AvroIOSpec extends FlatSpec with Matchers {


  ignore should "be serializable to disk in binary format" in {

    val sc = SparkTestHelper.getLocalSparkContext
    val data = List(new WindowStateChanged(12, 12, DateTime.now().getMillis, "fuck", "you", "asshole"))
    val events = sc.parallelize(data)
//    val mapped = events.map{
//      case e:Event => e.data match {
//        case off:ScreenOff => AvroType[ScreenOff].io.
//      }
//    }
    val path = "./src/test/resources/avro-io-results"
    File(path).deleteIfExists()
    events.saveAsObjectFile(path)
    //rdds.saveAsTextFile("./src/test/resources/avro-io-results")
  }

//  "RDDs of Events" should "be serializable to disk in binary format" in {
  ignore should "be serializable to disk in binary format 2" in {

    val sc = SparkTestHelper.getLocalSparkContext
    val data = Seq(
      new WindowStateChanged(12, 12, DateTime.now().getMillis, "fuck", "you", "asshole"),
      new WindowStateChanged(13, 15, DateTime.now().getMillis, "fuck", "jau", "asshole")
    )
    val events = sc.parallelize(data).map( (1, _) )
    val path = "./src/test/resources/avro-io-results"
    File(path).deleteRecursively()
    events.saveAsObjectFile(path)
  }

  it should "be serializable to disk in binary format 3" in {
    val sc = SparkTestHelper.getLocalSparkContext
    val data = Seq(new AppInstall(1,2,3,"appName", "pkgName") )
    val events = sc.parallelize(data).map( (1, _) )
    val path = "./src/test/resources/avro-io-results"
    File(path).deleteRecursively()
    events.saveAsObjectFile(path)
  }
}

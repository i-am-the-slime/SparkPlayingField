package org.menthal

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import org.menthal.model._
import com.gensler.scalavro.types.AvroType
import org.joda.time.DateTime
import org.menthal.model.events._
import org.menthal.model.events.EventData._
import org.scalatest.{Matchers, FlatSpec}

import org.menthal.model.events.Event
import scala.reflect.io.File
import scala.util.Success
import org.menthal.model.events._
import org.apache.spark.SparkContext._

import parquet.avro.{AvroWriteSupport, AvroParquetOutputFormat}
import parquet.hadoop.ParquetOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.Schema

class AvroIOSpec extends FlatSpec with Matchers {


  ignore should "be serializable to disk in binary format" in {

    val sc = SparkTestHelper.getLocalSparkContext
    val data = List(Event(12, 12, DateTime.now().getMillis, WindowStateChanged("fuck", "you", "asshole")))
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

  ignore should "read and write Events as JSON" in {
    val io = AvroType[Event].io
    val out = new ByteArrayOutputStream

    val event = new Event(12,12,12,ScreenOff())
    io.write(event, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(event))
  }

  it should "read and write ScreenOff as JSON" in {
    val io = AvroType[ScreenOff].io
    val out = new ByteArrayOutputStream

    val screenOff = ScreenOff()
    io.write(screenOff, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(screenOff))
  }

  it should "read and write WindowStateChange as JSON" in {
    val io = AvroType[WindowStateChanged].io
    val out = new ByteArrayOutputStream

    val wsc = WindowStateChanged("hey", "you", "o")
    io.write(wsc, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(wsc))
  }

//  "RDDs of Events" should "be serializable to disk in binary format" in {
  ignore should "be serializable to disk in binary format 2" in {

    val sc = SparkTestHelper.getLocalSparkContext
    val data = Seq(
      Event(12, 12, DateTime.now().getMillis, WindowStateChanged("fuck", "you", "asshole")),
      Event(13, 15, DateTime.now().getMillis, WindowStateChanged("fuck", "jau", "asshole"))
    )
    val events = sc.parallelize(data).map( (1, _) )
    val path = "./src/test/resources/avro-io-results"
    File(path).deleteRecursively()
    events.saveAsObjectFile(path)
  }

  it should "be serializable to disk in binary format 3" in {

    val sc = SparkTestHelper.getLocalSparkContext
    val data = Seq( AppInstall(1,2,3,"appName", "pkgName") )
    val events = sc.parallelize(data).map( (1, _) )
    val path = "./src/test/resources/avro-io-results"
    File(path).deleteRecursively()
    events.saveAsObjectFile(path)
  }

  it should "use parquet" in {
    val sc = SparkTestHelper.getLocalSparkContext
    val job = new Job
    // Configure the ParquetOutputFormat to use Avro as the serialization format
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    // You need to pass the schema to AvroParquet when you are writing objects but not when you
    // are reading them. The schema is saved in Parquet file for future readers to use.
    //val avroSchema = AvroType[jevents.AppInstall].schema().compactPrint
    val schema = AppInstall.getSchema()
    AvroParquetOutputFormat.setSchema(job, schema)
    // Create a PairRDD with all keys set to null and wrap each amino acid in serializable objects
    val data = List(AppInstall(1, 2, 3,"s", "a"), AppInstall(1, 2, 3, "s", "a"))
    val events = sc.parallelize(data)
    val pairs = events.map( (null, _) )
    // Save the RDD to a Parquet file in our temporary output directory
    val path = "./src/test/resources/api_hadoop_file"
    File(path).deleteRecursively()
    pairs.saveAsNewAPIHadoopFile(path, classOf[Void], classOf[AppInstall],
      classOf[ParquetOutputFormat[AppInstall]], job.getConfiguration)
  }

  it should "please work" in {
//    def writeAvroFile[T <: SpecificRecord]( file: File,
//                                            factoryMethod: Int => T,
//                                            count: Int): Unit = {
//      val prototype = factoryMethod(0)
//      val datumWriter = new SpecificDatumWriter[T](
//        prototype.getClass.asInstanceOf[java.lang.Class[T]])
//      val dataFileWriter = new DataFileWriter[T](datumWriter)
//
//      dataFileWriter.create(prototype.getSchema, file)
//      for(i <- 1 to count) {
//        dataFileWriter.append(factoryMethod(i))
//      }
//      dataFileWriter.close()
//    }
  }
}

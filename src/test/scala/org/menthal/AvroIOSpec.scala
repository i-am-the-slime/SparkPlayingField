package org.menthal

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import com.gensler.scalavro.types.AvroType
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}
import org.apache.avro.{SchemaBuilder, Schema}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.menthal.model.events._
import org.scalatest.{Matchers, FlatSpec}
import parquet.avro.{AvroWriteSupport, AvroParquetOutputFormat}
import parquet.hadoop.ParquetOutputFormat
import org.apache.spark.SparkContext._


import scala.reflect.io.File
import scala.util.Success

class AvroIOSpec extends FlatSpec with Matchers {


  "Avro" should "read and write ScreenOff as binary" in {
//  it should "read and write ScreenOff as binary" in {
    val io = AvroType[ScreenOff].io
    val out = new ByteArrayOutputStream

    val screenOff = ScreenOff()
    io.write(screenOff, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(screenOff))
  }

  it should "read and write WindowStateChange as binary" in {
    val io = AvroType[WindowStateChanged].io
    val out = new ByteArrayOutputStream

    val wsc = WindowStateChanged("hey", "you", "o")
    io.write(wsc, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(wsc))
  }

  it should "read and write a Sequence of EventData as binary" in {
    val x = AvroType[Seq[EventData]]
    val io = x.io

    val out = new ByteArrayOutputStream

    val stuff:Seq[EventData] = Seq(WindowStateChanged("","",""), ScreenOff())
    io.write(stuff, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(stuff))
  }

  ignore should "read and write a Sequence of Event as binary" in {
    val x = AvroType[Seq[Event]]
    val io = x.io

    val out = new ByteArrayOutputStream

    val stuff:Seq[Event] = Seq(Event(12,12,12, WindowStateChanged("","","")))
    io.write(stuff, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(stuff))
  }

//  "RDDs of Events" should "be serializable to disk in binary format" in {
  it should "be serializable to disk in binary format" in {

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

  it should "be deserializable from disk" in {

  }

  ignore should "use parquet" in {
    val sc = SparkTestHelper.getLocalSparkContext
    val job = new Job
    // Configure the ParquetOutputFormat to use Avro as the serialization format
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    // You need to pass the schema to AvroParquet when you are writing objects but not when you
    // are reading them. The schema is saved in Parquet file for future readers to use.
    val avroSchema = AvroType[Int].schema().compactPrint
    val schema = Schema.parse(avroSchema)
    AvroParquetOutputFormat.setSchema(job, schema)
    // Create a PairRDD with all keys set to null and wrap each amino acid in serializable objects
    val data = List( 1,2,4 )
    val events = sc.parallelize(data)
    val pairs = events.map( (null, _) )
    // Save the RDD to a Parquet file in our temporary output directory
    val path = "./src/test/resources/api_hadoop_file"
    File(path).deleteRecursively()
    pairs.saveAsNewAPIHadoopFile(path, classOf[Void], classOf[Int],
      classOf[ParquetOutputFormat[Int]], job.getConfiguration)
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

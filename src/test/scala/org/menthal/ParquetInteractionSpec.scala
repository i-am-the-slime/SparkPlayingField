package org.menthal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter._
import org.menthal.model._
import parquet.filter.ColumnPredicates._
import org.menthal.model.jevents._
import org.scalatest.{FlatSpec, Matchers}
import parquet.avro.{AvroReadSupport, AvroParquetOutputFormat, AvroWriteSupport}
import parquet.filter.{RecordFilter, UnboundRecordFilter}
import parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}
import org.apache.spark.SparkContext._

import scala.reflect.io.File

class ParquetInteractionSpec extends FlatSpec with Matchers{

  "Our system" should "read and write AppInstall events to parquet" in {
    val sc = SparkTestHelper.getLocalSparkContext
    val job = Job.getInstance(new Configuration)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])

    val install1 = new AppInstall("appName1", "pkgName1")
    val install2 = new AppInstall("appName2", "pkgName2")
    val data = List( install1, install2 )
    val events = sc.parallelize(data)
    val pairs = events.map( (null, _) )

    val schema = AppInstall.SCHEMA$
    AvroParquetOutputFormat.setSchema(job, schema)

    val path = "./src/test/resources/app_install_parquet_test"

    pairs.saveAsNewAPIHadoopFile(path, classOf[Void], classOf[AppInstall],
      classOf[ParquetOutputFormat[AppInstall]], job.getConfiguration)

    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AppInstall]])
    val file = sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[AppInstall]],
      classOf[Void], classOf[AppInstall], job.getConfiguration)

    ParquetInputFormat.setUnboundRecordFilter(job, classOf[ParquetInteractionSpec.SimpleFilter])
    val filteredFile = sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[AppInstall]],
      classOf[Void], classOf[AppInstall], job.getConfiguration)
    filteredFile.foreach(ParquetInteractionSpec.assertionFunction)

    //Cleanup
    File(path).deleteRecursively()
    sc.stop()
  }

  it should "read and write events using parquet" in {
    //Setup
    val sc = SparkTestHelper.getLocalSparkContext
    val job = Job.getInstance(new Configuration)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])

    //Test
    val data = List(
      new Event(1,1,1, new WindowStateChanged("appName", "pkgName", "title")),
      new Event(2,2,2, new AppInstall("appName", "pkgName"))
    )
    val events = sc.parallelize(data)
    val pairs = events.map( (null, _) )

    val schema = Event.SCHEMA$
    AvroParquetOutputFormat.setSchema(job, schema)

    val path = "./src/test/resources/events_parquet_test"

    pairs.saveAsNewAPIHadoopFile(path, classOf[Void], classOf[Event],
      classOf[ParquetOutputFormat[Event]], job.getConfiguration)

    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[Event]])
    val file = sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[Event]],
      classOf[Void], classOf[Event], job.getConfiguration)
//
//    ParquetInputFormat.setUnboundRecordFilter(job, classOf[ParquetInteractionSpec.SimpleFilter])
//    val filteredFile = sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[Event]],
//      classOf[Void], classOf[Event], job.getConfiguration)
//    filteredFile.foreach(ParquetInteractionSpec.assertionFunction2)
//
    //Cleanup
    File(path).deleteRecursively()
    sc.stop()
  }
}

object ParquetInteractionSpec extends Matchers {
  def assertionFunction(tuple: (Void, AppInstall)):Unit =
    if(tuple._2 != null)
      tuple._2 shouldBe new AppInstall("appName1", "pkgName1")

  def assertionFunction2(tuple: (Void, Event)):Unit =
    if(tuple._2 != null && tuple._2.getData.getClass == classOf[AppInstall])
      tuple._2 shouldBe new AppInstall("appName", "pkgName")

  class SimpleFilter extends UnboundRecordFilter {
    override def bind(readers: java.lang.Iterable[ColumnReader]): RecordFilter =
      column("appName", equalTo("appName1")).bind(readers)
  }
}

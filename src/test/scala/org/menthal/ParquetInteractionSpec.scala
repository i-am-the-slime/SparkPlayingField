package org.menthal

import org.apache.avro.Schema.{Parser => AvroParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.menthal.model.events.{CCAppInstall, WindowStateChanged, AppInstall}
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter._
import org.menthal.model._
import parquet.filter.ColumnPredicates._
import org.scalatest.{FlatSpec, Matchers}
import parquet.avro.{AvroReadSupport, AvroParquetOutputFormat, AvroWriteSupport}
import parquet.filter.{RecordFilter, UnboundRecordFilter}
import parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

import scala.reflect.io.File
import scala.util.Try

class ParquetInteractionSpec extends FlatSpec with Matchers{

  "Our system" should "read and write AppInstall events to parquet" in {
    val sc = SparkTestHelper.getLocalSparkContext
    val writeJob = Job.getInstance(new Configuration)
    ParquetOutputFormat.setWriteSupportClass(writeJob, classOf[AvroWriteSupport])

    val install1 = new AppInstall(1,2,3, "appName1", "pkgName1")
    val install2 = new AppInstall(2,3,4, "appName2", "pkgName2")
    val data = List( install1, install2 )
    val events:RDD[AppInstall] = sc.parallelize(data)
    val pairs = events.map( (null, _) )

    val f = new java.io.File("model/avro/app_install.avsc")
    val schema = new AvroParser().parse(f)
    AvroParquetOutputFormat.setSchema(writeJob, schema)

    val path = "./src/test/resources/app_install_parquet_test"
    Try(File(path).deleteRecursively())

    pairs.saveAsNewAPIHadoopFile(path, classOf[Void], classOf[AppInstall],
      classOf[ParquetOutputFormat[AppInstall]], writeJob.getConfiguration)

    val readJob = Job.getInstance(new Configuration)

    ParquetInputFormat.setReadSupportClass(readJob, classOf[AvroReadSupport[AppInstall]])
    val file = sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[AppInstall]],
      classOf[Void], classOf[AppInstall], readJob.getConfiguration)

//    ParquetInputFormat.setUnboundRecordFilter(readJob, classOf[ParquetInteractionSpec.SimpleFilter])

    val filteredFile = sc.newAPIHadoopFile(
      path,
      classOf[ParquetInputFormat[AppInstall]],
      classOf[Void],
      classOf[AppInstall],
      readJob.getConfiguration)
      .map(_._2.asInstanceOf[AppInstall])

    filteredFile.foreach(ParquetInteractionSpec.assertionFunction)

    //Cleanup
    File(path).deleteRecursively()
    sc.stop()
  }

  ignore should "write and read stuff with Spark's write to parquet" in {
    val path = "hey.parquet"
    File(path).deleteRecursively()

    val sc = SparkTestHelper.getLocalSparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    val events:List[CCAppInstall] = List(CCAppInstall(1,2,3,"boo", "galoo"))
    val rdds = sc.parallelize(events)

    val schemaRDDs = createSchemaRDD(rdds)
    schemaRDDs.saveAsParquetFile(path)
    val parquetFile = sqlContext.parquetFile(path)
    parquetFile.foreach(ParquetInteractionSpec.printEm)

    sc.stop()
  }
}

object ParquetInteractionSpec extends Matchers {
  def assertionFunction(install: AppInstall):Unit =
    System.out.println(install.toString)
//    if(tuple._2 != null)
//      tuple._2.asInstanceOf[AppInstall] shouldBe AppInstall(1,2,3,"appName1", "pkgName1")

//  def assertionFunction2(tuple: (Void, Event)):Unit =
//    if(tuple._2 != null && tuple._2.getData.getClass == classOf[AppInstall])
//      tuple._2 shouldBe new AppInstall("appName", "pkgName")
  def printEm(row:sql.Row) = System.out.println(row.toString())

  class SimpleFilter extends UnboundRecordFilter {
    override def bind(readers: java.lang.Iterable[ColumnReader]): RecordFilter =
      column("appName", equalTo("appName1")).bind(readers)
  }
}

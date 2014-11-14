package org.menthal

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.events.{CCWindowStateChanged, WindowStateChanged, AppInstall, CCAppInstall}
import org.menthal.model.EventType
import org.menthal.model.EventType._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import parquet.column.ColumnReader
import parquet.filter.{RecordFilter, UnboundRecordFilter}
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._


import scala.reflect.io.File
import scala.util.Try

class ParquetIOSpec extends FlatSpec with Matchers with BeforeAndAfterEach{

  import ParquetIOSpec._
  @transient var sc:SparkContext = _
  val path = "./src/test/resources/" + "ParquetIOTest"

  override def beforeEach(){
    sc = SparkTestHelper.localSparkContext
    Try(File(path).deleteRecursively())
  }

  override def afterEach() = {
//    Try(File(path).deleteRecursively())
    sc.stop()
    sc = null
  }

  "The ParquetIO class" should "read and write RDDs of AppSession" in {
    val data = sc.parallelize(Seq(
      new AppInstall(1L, 2L, 3L, "appName", "pkgName"),
      new AppInstall(7L, 8L, 11L, "frederik", "209")
    ))
    ParquetIO.write(sc, data, path, AppInstall.getClassSchema)
    val readResult = ParquetIO.read(path, sc)
    readResult zip data foreach ParquetIOSpec.compareThem
  }

  "The ParquetIO class" should "read and write RDDs of WindowStateChanged" in {
    val data = sc.parallelize(Seq(
      new WindowStateChanged(1L,2L,3L, "appName", "pkgName", KNACKWURST),
      new WindowStateChanged(7L,8L,11L, "frederik", "209", "schnarbeltir")
    ))
    ParquetIO.write(sc, data, path, WindowStateChanged.getClassSchema)
    val readResult = ParquetIO.read(path, sc)

    readResult zip data foreach ParquetIOSpec.compareThem
  }

  //ignore should "apply UnboundRecordFilters" in {
  "The ParquetIO class" should "apply UnboundRecordFilters" in {
    val data = sc.parallelize(Seq(
      new WindowStateChanged(1L, 2L, 3L, "appName", "pkgName", KNACKWURST),
      new WindowStateChanged(7L, 8L, 11L, "frederik", "209", "slllllljltir")
    ))
    ParquetIO.write(sc, data, path, WindowStateChanged.getClassSchema)

    val filteredData = Seq(
      new WindowStateChanged(1L, 2L, 3L, "appName", "pkgName", KNACKWURST)
    )

    val readResult = ParquetIO.read(path, sc, Some(classOf[ParquetIOSpec.SomeFilter])).collect()
    readResult equals filteredData

  }

  "The filterAndWriteToParquet() function" should "Write simple RDD of menthal Events to Parquet Correctly" in {
    val data = sc.parallelize(Seq(
      new CCWindowStateChanged(1L, 2L, 3L, "appName", "pkgName", KNACKWURST),
      new CCWindowStateChanged(7L, 8L, 11L, "frederik", "209", "slllllljltir")
    ))
    ParquetIO.filterAndWriteToParquet(sc, data, TYPE_WINDOW_STATE_CHANGED, path)

    val readData = sc.parallelize(Seq(
      new WindowStateChanged(1L, 2L, 3L, "appName", "pkgName", KNACKWURST),
      new WindowStateChanged(7L, 8L, 11L, "frederik", "209", "slllllljltir")
    ))

    val readResult = ParquetIO.read(path + "/" + EventType.toPath(TYPE_WINDOW_STATE_CHANGED), sc)
    readResult zip readData foreach ParquetIOSpec.compareThem
  }
}

object ParquetIOSpec extends Matchers{
  val KNACKWURST = "knackwurst"

  def compareThem(tup:(Any, Any)) = {
    tup._1 shouldBe tup._2
  }

  class SomeFilter extends UnboundRecordFilter {
    def bind(readers: java.lang.Iterable[ColumnReader]): RecordFilter = {
      column("appName", equalTo("appName")).bind(readers)
    }
  }
}

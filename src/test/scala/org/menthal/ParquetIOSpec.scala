package org.menthal

import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.menthal.model.events.{WindowStateChanged, AppInstall, CCAppInstall}
import org.menthal.model.serialization.ParquetIO
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import parquet.column.ColumnReader
import parquet.filter.{RecordFilter, UnboundRecordFilter}
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._


import scala.reflect.io.File
import scala.util.Try

class ParquetIOSpec extends FlatSpec with Matchers with BeforeAndAfterEach{

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
      new AppInstall(1,2,3, "appName", "pkgName"),
      new AppInstall(7,8,11, "frederik", "209")
    ))
    ParquetIO.write(sc, data, path, AppInstall.getClassSchema)
    val readResult = ParquetIO.read(path, sc)
    readResult zip data foreach ParquetIOSpec.compareThem
  }

  "The ParquetIO class" should "read and write RDDs of WindowStateChanged" in {
    val data = sc.parallelize(Seq(
      new WindowStateChanged(1,2,3, "appName", "pkgName", "knackwrust"),
      new WindowStateChanged(7,8,11, "frederik", "209", "schnarbeltir")
    ))
    ParquetIO.write(sc, data, path, WindowStateChanged.getClassSchema)
    val readResult = ParquetIO.read(path, sc)

    readResult zip data foreach ParquetIOSpec.compareThem
  }

  "The ParquetIO class" should "apply UnboundRecordFilters" in {
    val data = sc.parallelize(Seq(
      new WindowStateChanged(1,2,3, "appName", "pkgName", "knackwrust"),
      new WindowStateChanged(7,8,11, "frederik", "209", "slllllljltir")
    ))
    ParquetIO.write(sc, data, path, WindowStateChanged.getClassSchema)

    val filteredData = sc.parallelize(Seq(
      new WindowStateChanged(1,2,3, "appName", "pkgName", "knackwrust")
    ))

    val readResult = ParquetIO.read(path, sc, Some(classOf[ParquetIOSpec.SomeFilter]))

    readResult zip filteredData foreach ParquetIOSpec.compareThem

  }

}

object ParquetIOSpec extends Matchers{
  def compareThem(tup:(Any, Any)) = {
    tup._1 shouldBe tup._2
  }

  class SomeFilter extends UnboundRecordFilter {
    def bind(readers: java.lang.Iterable[ColumnReader]): RecordFilter = {
      column("windowTitle", equalTo("knackwrust")).bind(readers)
    }
  }
}

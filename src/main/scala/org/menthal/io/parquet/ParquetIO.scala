package org.menthal.io.parquet

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.menthal.model.events.{CCScreenOn, MenthalEvent}
import org.menthal.model.EventType
import parquet.avro._
import parquet.filter.UnboundRecordFilter
import parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}

import scala.reflect.ClassTag
import scala.util.Try

object ParquetIO {


  def filterAndWriteToParquet(sc:SparkContext, events: RDD[_ <:MenthalEvent], eventType: Int, dirPath:String ) = {
    val filteredEvents = events.filter  {e => (EventType.fromMenthalEvent(e) == eventType) }.map(_.toAvro)
    val path = s"$dirPath/${EventType.toPath(eventType)}"
    val schema = EventType.toSchema(eventType)
    ParquetIO.write(sc, filteredEvents, path, schema)
  }

//  def MapByTypesAndWriteToParquet(sc:SparkContext, events: RDD[_ <:MenthalEvent], eventType: Int, dirPath:String ) = {
//    val filteredEvents = events.map(e => (EventType.fromMenthalEvent(e), e))
//    val filterdEvents =
////    val path = s"$dirPath/${EventType.toPath(eventType)}"
//    val schema = EventType.toSchema(eventType)
//    ParquetIO.write(sc, filteredEvents, path, schema)
//  }



  def readEventType[A <: SpecificRecord](
    path: String,
    eventType: Int,
    sc: SparkContext,
    recordFilter:Option[Class[_ <: UnboundRecordFilter]]=None)
      (implicit ct:ClassTag[A]): RDD[A] = {
    read(path + "/" + EventType.toPath(eventType),sc, recordFilter)(ct)
  }

  def write[A <: SpecificRecord](sc: SparkContext, data: RDD[A], path: String, schema:Schema)(implicit ct:ClassTag[A]) = {
    //val isEmpty = data.fold(0)
    val isEmpty = Try(data.first).isFailure

    if (!isEmpty) {
      val writeJob = Job.getInstance(new Configuration)
      ParquetOutputFormat.setWriteSupportClass(writeJob, classOf[AvroWriteSupport])
      val pairs: RDD[(Void, A)] = data.map((null, _))
      AvroParquetOutputFormat.setSchema(writeJob, schema)

      pairs.saveAsNewAPIHadoopFile(
        path,
        classOf[Void],
        ct.runtimeClass,
        classOf[ParquetOutputFormat[A]],
        writeJob.getConfiguration)

    }
  }

  def read[A <: SpecificRecord](path: String, sc: SparkContext, recordFilter:Option[Class[_ <: UnboundRecordFilter]]=None)(implicit ct:ClassTag[A]): RDD[A] = {

    val readJob = Job.getInstance(new Configuration)
    ParquetInputFormat.setReadSupportClass(readJob, classOf[AvroReadSupport[A]])

    if(recordFilter.isDefined){
      ParquetInputFormat.setUnboundRecordFilter(readJob, recordFilter.get)
    }

    val filteredFile = sc.newAPIHadoopFile(
      path,
      classOf[ParquetInputFormat[A]],
      classOf[Void],
      ct.runtimeClass.asInstanceOf[Class[A]],
      readJob.getConfiguration)
      .map(_._2.asInstanceOf[A])


    filteredFile.asInstanceOf[RDD[CCScreenOn]].collect().sortBy(_.time)
    filteredFile
  }
}

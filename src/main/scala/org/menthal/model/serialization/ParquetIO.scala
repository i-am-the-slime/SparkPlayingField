package org.menthal.model.serialization

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.menthal.model.events.DreamingStarted
import parquet.avro._
import parquet.filter.UnboundRecordFilter
import parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}

import scala.reflect.ClassTag

object ParquetIO {


  def writeDreamingStarted(sc:SparkContext, data:RDD[DreamingStarted], path:String) = {
    val isEmpty = data.mapPartitions(iter => Iterator(!iter.hasNext)).reduce(_ && _)
    if(!isEmpty) {
      val writeJob = Job.getInstance(new Configuration)
      ParquetOutputFormat.setWriteSupportClass(writeJob, classOf[AvroWriteSupport])
      val pairs: RDD[(Void, DreamingStarted)] = data.map((null, _))
      AvroParquetOutputFormat.setSchema(writeJob, data.first().getSchema)
      pairs.saveAsNewAPIHadoopFile(
        path,
        classOf[Void],
        classOf[DreamingStarted],
        classOf[ParquetOutputFormat[DreamingStarted]],
        writeJob.getConfiguration)
    }
  }

  def write[A <: SpecificRecord](sc: SparkContext, data: RDD[A], path: String, schema:Schema)(implicit ct:ClassTag[A]) = {
    val isEmpty = data.mapPartitions(iter => Iterator(! iter.hasNext)).reduce(_ && _)
    if(!isEmpty) {
      val writeJob = Job.getInstance(new Configuration)
      ParquetOutputFormat.setWriteSupportClass(writeJob, classOf[AvroWriteSupport])
      val pairs: RDD[(Void, A)] = data.map((null, _))

      AvroParquetOutputFormat.setSchema(writeJob, DreamingStarted.SCHEMA$)

      pairs.saveAsNewAPIHadoopFile(
        path,
        classOf[Void],
        classOf[DreamingStarted],
        classOf[ParquetOutputFormat[DreamingStarted]],
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

    filteredFile
  }
}

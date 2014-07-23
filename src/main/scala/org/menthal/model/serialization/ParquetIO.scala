package org.menthal.model.serialization

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.file.DataFileWriter
import org.apache.avro.reflect.AvroSchema
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.menthal.model.events.{AppInstall, MenthalEvent}
import parquet.avro._
import parquet.filter.UnboundRecordFilter
import parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}

import scala.reflect.ClassTag
import scala.reflect.io.File
import scala.util.Try

object ParquetIO {

  def write[A <: SpecificRecord](sc: SparkContext, data: RDD[A], path: String, schema: Schema)(implicit ct:ClassTag[A]) = {
    val writeJob = Job.getInstance(new Configuration)
    ParquetOutputFormat.setWriteSupportClass(writeJob, classOf[AvroWriteSupport])

    val pairs:RDD[(Void, A)] = data.map((null, _))

    AvroParquetOutputFormat.setSchema(writeJob, schema)

    pairs.saveAsNewAPIHadoopFile(
      path,
      classOf[Void],
      ct.runtimeClass,
      classOf[ParquetOutputFormat[A]],
      writeJob.getConfiguration)

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

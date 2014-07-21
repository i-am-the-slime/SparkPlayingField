package org.menthal.model.serialization

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.menthal.model.events.MenthalEvent

object FileIO {
//  def write[A : SpecificRecord](data:RDD[A], path:String) = {
//    val baos = new ByteArrayOutputStream()
//    val writer = new SpecificDatumWriter[A](classOf[A])
//    val dataFileWriter = new DataFileWriter[A](writer)
//    data.map(_ ).saveAsNewAPIHadoopFile(path)
//  }
//
//  def read[A : MenthalEvent](path:String, sc:SparkContext):RDD[A] = {
//    sc.textFile(path)
//  }
}

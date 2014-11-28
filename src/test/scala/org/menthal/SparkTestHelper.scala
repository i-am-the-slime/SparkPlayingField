package org.menthal

import java.util.logging.{Level, Logger}

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.menthal.io.serialization.MenthalKryoRegistrator

object SparkTestHelper {
  def localSparkContext: SparkContext = {
    val parquetHadoopLogger = Logger.getLogger("parquet.hadoop")
    parquetHadoopLogger.setLevel(Level.SEVERE)
    val conf = new SparkConf()
      .setMaster("local")
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .set("spark.kryo.registrator", classOf[MenthalKryoRegistrator].getCanonicalName)
      .set("spark.kryo.referenceTracking", "false")
      .setAppName("test")
      .set("spark.executor.memory", "512M")
    val sc = new SparkContext(conf)
    sc
  }
}

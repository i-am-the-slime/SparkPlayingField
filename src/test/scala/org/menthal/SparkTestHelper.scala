package org.menthal

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

object SparkTestHelper {
  def localSparkContext: SparkContext = {
    val parquetHadoopLogger = Logger.getLogger("parquet.hadoop")
    parquetHadoopLogger.setLevel(Level.SEVERE)
    val conf = new SparkConf()
      .setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.menthal.model.serialization.MenthalKryoRegistrator")
      .set("spark.kryo.referenceTracking", "false")
      .setAppName("test")
      .set("spark.executor.memory", "512M")
    val sc = new SparkContext(conf)
    sc
  }
}

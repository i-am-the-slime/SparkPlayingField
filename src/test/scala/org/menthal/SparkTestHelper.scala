package org.menthal

import org.apache.spark.{SparkConf, SparkContext}

object SparkTestHelper {
  def localSparkContext: SparkContext = {
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

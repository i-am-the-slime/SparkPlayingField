package org.menthal

import org.apache.spark.{SparkConf, SparkContext}

object SparkTestHelper {
  def getLocalSparkContext: SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "org.menthal.model.serialization.MenthalKryoRegistrator")
      .setAppName("NewAggregationsSpec")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc
  }

}

package org.menthal.spark

import org.apache.spark.SparkContext

/**
 * Created by mark on 24.10.2014.
 */
object SparkHelper {

  def getSparkContext(master:String, name: String) = {
    new SparkContext(master,
      name,
      System.getenv("SPARK_HOME"),
      Nil,
      Map(
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator" -> "org.menthal.model.serialization.MenthalKryoRegistrator",
        "spark.kryo.referenceTracking" -> "false")
    )
  }

}

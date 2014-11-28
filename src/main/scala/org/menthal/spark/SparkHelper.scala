package org.menthal.spark

import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import org.menthal.io.serialization.MenthalKryoRegistrator

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
        "spark.serializer" -> classOf[KryoSerializer].getCanonicalName,
        "spark.kryo.registrator" -> classOf[MenthalKryoRegistrator].getCanonicalName,
        "spark.kryo.referenceTracking" -> "false")
    )
  }

}

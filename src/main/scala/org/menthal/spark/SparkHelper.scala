package org.menthal.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.menthal.io.serialization.MenthalKryoRegistrator

/**
 * Created by mark on 24.10.2014.
 */
object SparkHelper {

//  def getSparkContextNewSpark(master:String, name: String):SparkContext = {
//    val conf = new SparkConf().setMaster(master).setAppName(name)
//    MenthalKryoRegistrator.registerClasses(conf)
//    new SparkContext(conf)
//  }


  def getSparkContext(master:String, name: String) = {
    new SparkContext(master,
      name,
      System.getenv("SPARK_HOME"),
      Nil,
      Map(
        "spark.serializer" -> classOf[KryoSerializer].getCanonicalName,
        "spark.kryo.registrator" -> classOf[MenthalKryoRegistrator].getCanonicalName,
        "spark.kryo.referenceTracking" -> "false"
  )
    )
  }

  def getSparkContext(master:String, name: String, env: Map[String,String]) = {
    val completeEnv =  Map(
      "spark.serializer" -> classOf[KryoSerializer].getCanonicalName,
      "spark.kryo.registrator" -> classOf[MenthalKryoRegistrator].getCanonicalName,
      "spark.kryo.referenceTracking" -> "false") ++ env

    new SparkContext(master,
      name,
      System.getenv("SPARK_HOME"),
      Nil,
      completeEnv)
  }


}

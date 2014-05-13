package org.menthal
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/home/hduser/spark/README.md" // Should be some file on your system
    val sc = new SparkContext("local", "Simple App", "/home/hduser/spark", List("target/scala-2.10/data-import_2.10-0.1.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

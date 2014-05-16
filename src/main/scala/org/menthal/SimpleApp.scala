package org.menthal
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SimpleApp <master> [<slices>]")
      System.exit(1)
    }
    val logFile = "/events.dump" // Should be some file on your system
    val sc = new SparkContext(args(0 ), "SimpleApp",
        System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    sc.stop()
  }

}

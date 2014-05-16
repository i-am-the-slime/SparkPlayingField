package org.menthal

import scala.math.random
import org.apache.spark._


/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "SparkPi",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val slices = if (args.length > 1) args(1).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
package org.menthal

import org.apache.spark._

/**
 * Created by konrad on 16.05.2014.
 */
object AppSessionAggregation {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SimpleApp <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SimpleApp",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val dumpFile = "/events.dump" // Should be some file on your system
    val eventsDump = sc.textFile(dumpFile,2).cache()
    val events = eventsDump.map(line => line.split("\t"))
    val appSessions = events.filter(event => event(3) == "3000")

    appSessions.saveAsTextFile("/results.txt")
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    sc.stop()
  }
}

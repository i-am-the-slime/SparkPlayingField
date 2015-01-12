package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.Granularity
import org.menthal.model.events.AppSession

/**
 * Created by mark on 09.01.15.
 */
object CombineAppSessions {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: Aggregations <master> <input1> <input2> <output>")
      System.exit(1)
    }
    val (master, input1, input2, output) = (args(0), args(1), args(2), args(3))
    val sc = new SparkContext(master, "Combine AppSessions", System.getenv("SPARK_HOME"))
    combineAndWrite(input1, input2, output, sc)
    sc.stop()
  }

  def combineAndWrite(input1:String, input2:String, output: String,  sc: SparkContext) = {
    def read(inputPath:String) = ParquetIO.read[AppSession](inputPath, sc)
    val combined = read(input1) ++ read(input2)
    ParquetIO.write(sc, combined, output, AppSession.getClassSchema)
  }

}

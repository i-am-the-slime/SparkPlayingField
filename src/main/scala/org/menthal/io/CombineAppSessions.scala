package org.menthal.io

import org.apache.spark.SparkContext
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.EventType
import org.menthal.model.events.AppSession
import org.menthal.spark.SparkHelper.getSparkContext

/**
 * Created by mark on 09.01.15.
 */
object CombineAppSessions {
  def name = "CombineAppSession"
  def main(args: Array[String]) {
    val (master, input1, input2, output) = args match {
    case Array(m, i1, i2, o) =>
      (m, i1, i2, o)
    case Array(m, i1, i2) =>
      (m, i1, i2, i1)
    case _ =>
      val errorMessage = "First argument is master, second phone called app session data path, third generated app sessions path, optional fourth ouptut path"
      throw new IllegalArgumentException(errorMessage)
  }
    val sc = getSparkContext(master, name)
    combineAndWrite(sc, input1, input2, output)
    sc.stop()
  }

  def goodSessionFilter(appSession: AppSession): Boolean = {
    //TODO write proper filter
    return true
  }

  def combineAndWrite(sc: SparkContext, input1:String, input2:String, output: String) = {
    val phoneCollectedSessions= ParquetIO.readEventType[AppSession](sc, input1, EventType.TYPE_APP_SESSION)
    val calculatedSessions= ParquetIO.readEventType[AppSession](sc, input2, EventType.TYPE_APP_SESSION)
    val resonableCalculatedSessions = calculatedSessions filter goodSessionFilter
    val combined = phoneCollectedSessions ++ resonableCalculatedSessions
    ParquetIO.writeEventType(sc, output, EventType.TYPE_APP_SESSION, combined)
  }

}

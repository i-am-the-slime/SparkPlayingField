package org.menthal.io

import org.menthal.aggregations.{SummaryAggregation, CategoriesAggregation, AppSessionAggregation, GeneralAggregations}
import org.menthal.io.hdfs.HDFSFileService
import org.menthal.spark.SparkHelper.getSparkContext

/**
 * Created by konrad on 23.01.15.
 */
object RunAllFromDump {
  val name = "RunAllFromDump"
  def main(args: Array[String]) {
    val (master, dumpFile, outputDir) = args match {
      case Array(m, d, o) =>
        (m,d,o)
      case _ =>
        val errorMessage = "First argument is master, second input path, third argument is output path"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, name)
    HDFSFileService.createTmps(2) match {

      case tmpOutputDir::tmpAppSessionsPath::nil =>
        //TODO add coping of additional input files
        PostgresDumpToParquet.parseFromDumpAndWriteToParquet(sc, dumpFile, tmpOutputDir)
        AppSessionAggregation.aggregate(sc, tmpOutputDir, tmpAppSessionsPath)
        CombineAppSessions.combineAndWrite(sc, tmpOutputDir, tmpAppSessionsPath, tmpOutputDir)
        GeneralAggregations.aggregate(sc, tmpOutputDir)
        CategoriesAggregation.aggregate(sc, tmpOutputDir)
        SummaryAggregation.aggregate(sc, tmpOutputDir)
        HDFSFileService.forceRename(tmpOutputDir, outputDir)
      case _ =>
        val errorMessage = "Cannot create temporary directories"
        throw new RuntimeException(errorMessage)
    }
    sc.stop()

  }

}

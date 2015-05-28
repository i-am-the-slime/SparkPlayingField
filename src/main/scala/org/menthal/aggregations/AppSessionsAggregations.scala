package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.menthal.aggregations.tools.AggrSpec
import org.menthal.aggregations.tools.AggrSpec._
import org.menthal.io.hdfs.HDFSFileService
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.EventType._
import org.menthal.model.Granularity.Timestamp
import org.menthal.model.events.AppSession
import org.menthal.model.events.Implicits._
import org.menthal.model.implicits.DateImplicits._
import org.menthal.model.{AggregationType, EventType, Granularity}
import org.menthal.spark.SparkHelper.getSparkContext

import scala.math.min


/**
 * Created by mark on 09.01.15.
 */
object AppSessionsAggregations  {
  def name = "AppSessionsComplete"
  def main(args: Array[String]) {
    val (master, datadir) = args match {
    case Array(m, d) =>
      (m, d)
    case _ =>
      val errorMessage = "First argument is master, second phone called app session data path, third generated app sessions path, optional fourth ouptut path"
      throw new IllegalArgumentException(errorMessage)
  }
    val sc = getSparkContext(master, name)
    aggregate(sc, datadir)
    //combineOnly(sc, datadir)
    sc.stop()
  }

  val filterStart:Timestamp = new DateTime(2013, 1, 1, 0, 0).getMillis
  val filterEnd : Timestamp = DateTime.now().getMillis

  def goodSessionFilter(appSession: AppSession): Boolean = {
    val time = appSession.getTime
    val durationInS = appSession.getDuration / 1000
    val condition =  (time > filterStart) && (time < filterEnd) && (durationInS < 3600)
    condition
  }

  def movePhoneCollectedSessions(sc: SparkContext, datadir:String) = {
    val originalSessionsPath = ParquetIO.pathFromEventType(datadir, EventType.TYPE_APP_SESSION)
    val newSessionsPath = phoneAppSessionsPath(datadir)
    HDFSFileService.rename(originalSessionsPath, newSessionsPath)
  }

  def phoneAppSessionsPath = alternateAppSessionsPath("_phone") _
  def calculatedAppSessionsPath = alternateAppSessionsPath("_calculated") _
  //def filteredAppSessionsPath = alternateAppSessionsPath("_filtered")
  def alternateAppSessionsPath(suffix: String)(datadir: String): String = ParquetIO.pathFromEventType(datadir, EventType.TYPE_APP_SESSION) + suffix


  def aggregate(sc: SparkContext, datadir:String) = {
    movePhoneCollectedSessions(sc, datadir)
    val phoneCollectedSessions: RDD[AppSession] = ParquetIO.read(sc, phoneAppSessionsPath(datadir))
    val calculatedAppSessions = AppSessionAggregation.parquetToAppSessions(sc, datadir) filter goodSessionFilter
    calculatedAppSessions.cache()
    ParquetIO.overwrite(sc, calculatedAppSessions, calculatedAppSessionsPath(datadir), AppSession.getClassSchema)
    val finalSessions = combineAppSessions(sc, phoneCollectedSessions, calculatedAppSessions)
    ParquetIO.writeEventType(sc, datadir, EventType.TYPE_APP_SESSION, finalSessions, overwrite = true)
  }

  def combineOnly(sc: SparkContext, datadir:String) = {
    val phoneCollectedSessions: RDD[AppSession] = ParquetIO.read(sc, phoneAppSessionsPath(datadir))
    val calculatedAppSessions: RDD[AppSession] = ParquetIO.read(sc, calculatedAppSessionsPath(datadir))
    val finalSessions = combineAppSessions(sc, phoneCollectedSessions, calculatedAppSessions)
    ParquetIO.writeEventType(sc, datadir, EventType.TYPE_APP_SESSION, finalSessions, overwrite = true)
  }

  def combineAppSessions(sc:SparkContext, phoneCollectedSessions: RDD[AppSession], calculatedAppSessions: RDD[AppSession]): RDD[AppSession] = {
    val filteredPhoneSessions = phoneCollectedSessions filter goodSessionFilter
    val sessionStarts = filteredPhoneSessions.map(s => (s.getUserId, s.getTime)).reduceByKey(min(_, _))
    val phoneCollectionStarts = sessionStarts.collectAsMap()
    val broadcastStarts = sc.broadcast(phoneCollectionStarts)

    def timeBeforePhoneCollectionStart(s:AppSession):Boolean =
      if (broadcastStarts.value.contains(s.getUserId))
        s.getTime < broadcastStarts.value(s.getUserId)
      else true
    val filteredCalculatedSessions = calculatedAppSessions filter timeBeforePhoneCollectionStart
    filteredPhoneSessions ++ filteredCalculatedSessions
  }

  def aggregateFurtherFromAppSessions(sc : SparkContext, datadir:String, lookupFile: String) = {
    val suite = List(AggrSpec(TYPE_APP_SESSION, toCCAppSession _, countAndDuration(AggregationType.AppTotalDuration, AggregationType.AppTotalCount)))
    AggrSpec.aggregate(sc, datadir, suite, Granularity.fullGranularitiesForest)
    CategoriesAggregation.aggregateCategories(sc, datadir, lookupFile)
  }

}

package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}
import org.joda.time.DateTime
import org.menthal.aggregations.tools.AggrSpec
import org.menthal.aggregations.tools.AggrSpec._
import org.menthal.io.hdfs.HDFSFileService
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.EventType._
import org.menthal.model.Granularity.Timestamp
import org.menthal.model.events.{CCAppSession, AppSession}
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
    val sqlContext = SQLContext.getOrCreate(sc)
    aggregate(sc, datadir)
    //combineOnly(sc, datadir)
    sc.stop()
  }

  val filterStart:Timestamp = new DateTime(2013, 1, 1, 0, 0).getMillis
  val filterEnd : Timestamp = DateTime.now().getMillis

  def goodSessionFilter(generated:Boolean)(appSession: AppSession): Boolean = {
    val time = appSession.getTime
    val durationInS:Long = if (generated) appSession.getDuration / 1000 else appSession.getDuration
    val condition =  (time > filterStart) && (time < filterEnd) && (durationInS < (12 * 3600))
    condition
  }




  def renameAppSessions(datadir:String, newSessionsPath: String) = {
    val originalSessionsPath = ParquetIO.pathFromEventType(datadir, EventType.TYPE_APP_SESSION)
    HDFSFileService.rename(originalSessionsPath, newSessionsPath)
  }

  def movePhoneCollectedSessions(datadir:String) =
    renameAppSessions(datadir, phoneAppSessionsPath(datadir))



  def phoneAppSessionsPath = alternateAppSessionsPath("_phone") _
  def calculatedAppSessionsPath = alternateAppSessionsPath("_calculated") _
  def filteredAppSessionsPath = alternateAppSessionsPath("_filtered") _
  def alternateAppSessionsPath(suffix: String)(datadir: String): String = ParquetIO.pathFromEventType(datadir, EventType.TYPE_APP_SESSION) + suffix


  def aggregate(sc: SparkContext, datadir:String) = {
    movePhoneCollectedSessions(datadir)
    val phoneCollectedSessions: RDD[AppSession] =
      ParquetIO.read(sc, phoneAppSessionsPath(datadir)) filter goodSessionFilter(generated = false)
    val calculatedAppSessions =
      AppSessionAggregation.parquetToAppSessions(sc, datadir) filter goodSessionFilter(generated = true)
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

  def filterPhoneOnly(sqlContext: SQLContext, datadir: String) = {
    movePhoneCollectedSessions(datadir)
    import sqlContext.implicits._
    val normalAppSessionPath = ParquetIO.pathFromEventType(datadir, EventType.TYPE_APP_SESSION)
    val phoneSessionsPath = phoneAppSessionsPath(datadir)
    val phoneCollectedSessions: Dataset[CCAppSession] = sqlContext.read.parquet(phoneSessionsPath).as[CCAppSession]
    val filteredAppSessions = phoneCollectedSessions.filter{ s =>
      (s.time > filterStart) && (s.time < filterEnd) && (s.duration < (12 * 3600))
    }
    filteredAppSessions.toDF().write.parquet(normalAppSessionPath)
    //renameAppSessions(sc, datadir, filteredAppSessionsPath(datadir))
  }

  def combineAppSessions(sc:SparkContext, phoneCollectedSessions: RDD[AppSession], calculatedAppSessions: RDD[AppSession]): RDD[AppSession] = {
    val filteredPhoneSessions = phoneCollectedSessions filter goodSessionFilter(generated = false)
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

//  def aggregateFurtherFromAppSessions(sc : SparkContext, datadir:String, lookupFile: String) = {
//    //val suite = List(AggrSpec(TYPE_APP_SESSION, toCCAppSession _, countAndDuration(AggregationType.AppTotalDuration, AggregationType.AppTotalCount)))
//    //AggrSpec.aggregate(suite, Granularity.fullGranularitiesForest)(sc, datadir)
//    //CategoriesAggregation.aggregateCategories(sc, datadir, lookupFile)
//  }

}

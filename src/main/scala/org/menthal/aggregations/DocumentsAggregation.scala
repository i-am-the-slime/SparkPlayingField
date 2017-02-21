package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, LongType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SQLContext}
import org.apache.spark.mllib.linalg.Vector
import org.menthal.io.hdfs.HDFSFileService
import org.menthal.model.Granularity
import org.menthal.spark.SparkHelper._


/**
 * Created by konrad on 04/11/15.
 */
object DocumentsAggregation {
  val name = "DocumentAggregation"

  def main(args: Array[String]) {
    val (master, datadir) = args match {
      case Array(m, d) =>  (m, d)
      case _ =>
        val errorMessage = "First argument is master, second input/output path"
        throw new IllegalArgumentException(errorMessage)
    }

    val sc = getSparkContext(master, name)
    createDocuments(sc, datadir)
    sc.stop()
  }

  def createDocuments(sc: SparkContext, datadir: String): Unit = {
    implicit val sqlContext = new SQLContext(sc)
    implicit val datadirPath = datadir
    //getDistData()
    //getMoodData()

    val personality =  sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("personality.csv")
    personality.registerTempTable(datadir + "/personality.csv")

    val phoneUsage = sqlContext.read.parquet(datadir + "/phone_sessions")
    phoneUsage.registerTempTable("phoneUsage")

    val userStats:DataFrame =
      if (!HDFSFileService.exists(datadirPath + "/userStats.csv")) getUserDayStats(personality)
      else sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true") // Automatically infer data types
        .load(datadirPath + "/userStats.csv")

    userStats.registerTempTable("userDayStats")
    userStats.write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Ignore)
      .option("header", "true")
      .save(datadirPath + "/userStats" + ".csv")


    val phoneSessions = filterSessions(phoneUsage, userStats)
    phoneSessions.registerTempTable("phoneSessions")
   // val locationSessions = filterSessions(sqlContext.read.parquet(datadir + "/locationSessions"), userStats)
   // locationSessions.registerTempTable("locationSessions")
    val activityClusters1 = filterSessions(sqlContext.read.parquet(datadir + "/activity_clusters_1"), userStats)
    activityClusters1.registerTempTable("activityClusters1")
   // val activityClusters2 = filterSessions(sqlContext.read.parquet(datadir + "/activity_clusters_2"), userStats)
//    activityClusters2.registerTempTable("activityClusters2")
//    val activityClusters3 = filterSessions(sqlContext.read.parquet(datadir + "/activity_clusters_3"), userStats)
//    activityClusters3.registerTempTable("activityClusters3")

    val appToNumber:DataFrame =
    if (!HDFSFileService.exists(datadir + "/appToNumber.csv")) getAppToNumber(sqlContext)
    else sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(datadir + "/appToNumber.csv")

    appToNumber.registerTempTable("appNames")
    appToNumber.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Ignore)
      .save(datadirPath + "/appToNumber" + ".csv")

//    val activityCountDocuments2 = getDocumentAggregates(activityClustersQuery(2, "COUNT"))
//    saveDocumentAggregates(activityCountDocuments2, "activity_count_documents_2")
//
//    val activityDurationDocuments2 = getDocumentAggregates(activityClustersQuery(2, "SUM"))
//    saveDocumentAggregates(activityDurationDocuments2, "activity_duration_documents_2")
//
//    val activityCountDocuments3 = getDocumentAggregates(activityClustersQuery(3, "COUNT"))
//    saveDocumentAggregates(activityCountDocuments3, "activity_count_documents_3")
//
//    val activityDurationDocuments3 = getDocumentAggregates(activityClustersQuery(3, "SUM"))
//    saveDocumentAggregates(activityDurationDocuments3, "activity_duration_documents_3")
//
val activityCountDocuments1 = getDocumentAggregates(activityUserAggregatesQuery(1, "COUNT"))
            saveDocumentAggregates(activityCountDocuments1, "user_activity_count_documents_2")

            val activityDurationDocuments1 = getDocumentAggregates(activityUserAggregatesQuery(1, "SUM"))
            saveDocumentAggregates(activityDurationDocuments1, "user_activity_duration_documents_1")

        val activityCountDocuments2 = getDocumentAggregates(activityUserAggregatesQuery(2, "COUNT"))
        saveDocumentAggregates(activityCountDocuments2, "user_activity_count_documents_2")

        val activityDurationDocuments2 = getDocumentAggregates(activityUserAggregatesQuery(2, "SUM"))
        saveDocumentAggregates(activityDurationDocuments2, "user_activity_duration_documents_2")

        val activityCountDocuments3 = getDocumentAggregates(activityUserAggregatesQuery(3, "COUNT"))
        saveDocumentAggregates(activityCountDocuments3, "user_activity_count_documents_3")

        val activityDurationDocuments3 = getDocumentAggregates(activityUserAggregatesQuery(3, "SUM"))
        saveDocumentAggregates(activityDurationDocuments3, "user_activity_duration_documents_3")

    val userDurationDocuments = getDocumentAggregates(userAggregatesQuery("COUNT"))
    saveDocumentAggregates(userDurationDocuments, "user_duration_documents")

    val userCountDocuments = getDocumentAggregates(userAggregatesQuery("SUM"))
    saveDocumentAggregates(userCountDocuments, "user_count_documents")

    val dailyCountDocuments = getDocumentAggregates(dailyAggregatesQuery("COUNT"))
    saveDocumentAggregates(dailyCountDocuments, "daily_count_documents")

    val dailyDurationDocuments =  getDocumentAggregates(dailyAggregatesQuery("SUM"))
    saveDocumentAggregates(dailyDurationDocuments, "daily_duration_documents")

//    val locationCountDocuments = getDocumentAggregates(locationAggregatesQuery("COUNT"))
//    saveDocumentAggregates(locationCountDocuments, "location_count_documents")
//
//    val locationDurationDocuments = getDocumentAggregates(locationAggregatesQuery("SUM"))
//    saveDocumentAggregates(locationDurationDocuments, "location_duration_documents")
//
//        val locationCountDocuments = getDocumentAggregates(locationUserAggregatesQuery("COUNT"))
//        saveDocumentAggregates(locationCountDocuments, "user_location_count_documents")
//
//        val locationDurationDocuments = getDocumentAggregates(locationUserAggregatesQuery("SUM"))
//        saveDocumentAggregates(locationDurationDocuments, "user_location_duration_documents")

    val unlockCountDocuments = getDocumentAggregates(phoneSessionAggregatesQuery("COUNT"))
    saveDocumentAggregates(unlockCountDocuments, "unlock_count_documents")

    val unlockDurationDocuments = getDocumentAggregates(phoneSessionAggregatesQuery("SUM"))
    saveDocumentAggregates(unlockDurationDocuments, "unlock_duration_documents")

  }

  def getDistData(sqlContext: SQLContext, datadir: String) = {
    val distanceCovered = sqlContext.read.parquet(datadir + "/distanceCovered")
    distanceCovered.registerTempTable("distanceCovered")
    val query = ("SELECT userId, day, dist FROM distanceCovered")
    sqlContext.sql(query)
  }

  def getAppToNumber(sqlContext:SQLContext):DataFrame = {
    val query = ("SELECT DISTINCT(app) from phoneSessions ORDER BY app")
    val appNumbers =for {
      (Row(app:String), index:Long) <- sqlContext.sql(query).rdd.zipWithIndex
    } yield Row(index, app)
    val schema  = StructType(List(StructField("index", LongType), StructField("app", StringType)))
    val appNames = sqlContext.createDataFrame(appNumbers, schema)
    appNames
  }

  case class CCDocumentString(id: Long, userId:Long, documentTime: Long, documentClusterType: Int, document: String)
  case class CCDocument(id: Long, userId:Long, documentTime: Long, documentClusterType: Int, document: List[(Int, Long)])
  case class CCDocumentVector(id: Long, userId:Long, documentTime: Long, documentClusterType: Int, document: Vector)


  def nextDayStart(time:Long):Long = Granularity.roundTimestampCeiling(time, Granularity.Daily)
  def dayStart(time:Long):Long = Granularity.roundTimestamp(time, Granularity.Daily)
  def daysBetween(time1: Long, time2: Long):Long = (time2 - time1) / (24 * 3600 * 1000)

  def getUserDayStats(personality: DataFrame)(implicit sqlContext: SQLContext):DataFrame = {
    sqlContext.udf.register("dayStart", dayStart _)
    sqlContext.udf.register("daysBetween", daysBetween _)
    sqlContext.udf.register("nextDayStart", nextDayStart _)
    val query = """SELECT *, daysBetween(startTime, endTime) as days FROM
                      (SELECT userId, nextDayStart(min(time)) as startTime, dayStart(max(time)) as endTime
                      FROM phoneUsage
                      GROUP BY userId) s"""
    val userDayStats = sqlContext.sql(query)
    userDayStats.join(personality, userDayStats("userId") <=> personality("user"), "inner")
                .select("userId", "startTime", "endTime", "days")
  }

  def filterSessions(df: DataFrame, userDayStats:DataFrame):DataFrame = {
    df.join(userDayStats, usingColumn = "userId")
      .filter("time >= startTime and time < endTime")
      .drop("startTime")
      .drop("endTime")
      .drop("days")
      //.select("userId", "documentTime", "documentCluster", "key", "value")
  }

  def getDocumentAggregates(query: String)(implicit sqlContext: SQLContext):RDD[CCDocument] = {
    val aggRdd = sqlContext.sql(query)
    val aggKVs = for {
      row <- aggRdd
      userId = row.getLong(0)
      documentTime = row.getLong(1)
      documentCluster = row.getInt(2)
      key = row.getInt(3)
      value = row.getLong(4)
    } yield  ((userId, documentTime, documentCluster), (key, value))
    val grouped = aggKVs.groupByKey
    val documents:RDD[CCDocument] = grouped.zipWithIndex map {
      case (((userId, documentTime, documentCluster), entries), index) =>
        CCDocument(index, userId, documentTime, documentCluster, entries.toList)
    }
    documents
  }


  def joinEntries(entries: List[(Int, Long)]):String = {
    entries.map { case (key, value) => s"[$key, $value]"} mkString("[", "," ,"]")
  }

  def saveDocumentAggregates(documents: RDD[CCDocument], name: String)(implicit sqlContext: SQLContext, datadirPath: String) = {
    import sqlContext.implicits._
    val documentStrings = documents.map {
      case CCDocument(index, userId, documentTime, documentClusterType, entries)
        => CCDocumentString(index, userId, documentTime, documentClusterType, joinEntries(entries))
    }

    val df: DataFrame = documentStrings.toDF
    df.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(datadirPath + "/"+ name + ".csv")
  }

  def phoneSessionAggregatesQuery(aggregate:String = "COUNT",
                                     lowerLimit:Int = 0, upperLimit:Int = 1000000):String = {
    s"""SELECT s.userId, s.sessionStart as documentTime, 0 as documentCluster, a.index as key, $aggregate(s.duration) as value
                FROM phoneSessions s JOIN appNames a
                WHERE s.app = a.app
                GROUP BY s.userId, s.sessionStart, a.index"""
  }

  def dailyAggregatesQuery(aggregate:String = "COUNT",
                         lowerLimit:Int = 0, upperLimit:Int = 1000000) =
    s"""SELECT s.userId, s.day as documentTime, 0 as documentCluster, a.index as key, $aggregate(duration) as value
                FROM (SELECT userId, time, time - (time % 86400000) as day, app, duration
                  FROM phoneSessions
                ) s JOIN appNames a
                WHERE s.app = a.app
                GROUP BY s.userId, s.day, a.index"""


  def locationAggregatesQuery(aggregate:String = "COUNT",
                            lowerLimit:Int = 0, upperLimit:Int = 1000000) =
    s"""SELECT l.userId, l.locationChangeTime as documentTime,
        CAST(l.locationCluster as Integer) as documentCluster, a.index as key, $aggregate(duration) as value
        FROM locationSessions l JOIN appNames a
        WHERE l.app = a.app
        GROUP BY l.userId, l.locationChangeTime, l.locationCluster, a.index"""


  def locationUserAggregatesQuery(aggregate:String = "COUNT",
                            lowerLimit:Int = 0, upperLimit:Int = 1000000) =
    s"""SELECT l.userId, CAST(0 as Long) as documentTime,
        CAST(l.locationCluster as Integer) as documentCluster, a.index as key, $aggregate(duration) as value
        FROM locationSessions l JOIN appNames a
        WHERE l.app = a.app
        GROUP BY l.userId, l.locationCluster, a.index"""


  def activityClustersQuery(n: Int, aggregate:String = "COUNT",
                          lowerLimit:Int = 0, upperLimit:Int = 1000000) =
    s"""SELECT s.userId, s.clusterStartTime as documentTime,
                  s.lastInactivitySessionRank as documentCluster, a.index as key, $aggregate(s.duration) as value
                FROM activityClusters$n s JOIN appNames a
                WHERE s.app = a.app
                GROUP BY s.userId, s.clusterStartTime, s.lastInactivitySessionRank, a.index"""
                //userId >$lowerLimit and userId <=$upperLimit

  def activityUserAggregatesQuery(n: Int, aggregate:String = "COUNT",
                            lowerLimit:Int = 0, upperLimit:Int = 1000000) =
    s"""SELECT s.userId, CAST(0 as Long) as documentTime,
                  s.lastInactivitySessionRank as documentCluster, a.index as key, $aggregate(s.duration) as value
                FROM activityClusters$n s JOIN appNames a
                WHERE s.app = a.app
                GROUP BY s.userId, s.lastInactivitySessionRank, a.index"""



  def userAggregatesQuery(aggregate:String = "COUNT",
                           lowerLimit:Int = 0, upperLimit:Int = 1000000) =
    s"""SELECT s.userId, CAST(0 as Long) as documentTime, 0 as documentCluster, a.index as key, $aggregate(duration) as value
                FROM phoneSessions s
                JOIN appNames a
                WHERE s.app = a.app
                GROUP BY s.userId, a.index"""


}

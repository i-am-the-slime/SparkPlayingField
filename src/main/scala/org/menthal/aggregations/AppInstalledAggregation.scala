package org.menthal.aggregations

import org.apache.spark.sql.SQLContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.menthal.aggregations.tools.myPackage.{RichSparkVector,RichBreezeVector}
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.{Granularity, AggregationType}
import org.menthal.model.EventType._
import org.menthal.model.events.{CCAggregationEntry, CCAppList, CCAppRemoval, CCAppInstall}
import org.menthal.spark.SparkHelper._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.{Map => MMap}

/**
 * Created by konrad on 01/07/16.
 */
object AppInstalledAggregation {
  def name = "AppInstalledAggregation"

  def main(args: Array[String]) {
    val (master, datadir) = args match {
      case Array(m, d) =>
        (m, d)
      case _ =>
        val errorMessage = "First argument is master, second phone called app session data path, third generated app sessions path, optional fourth ouptut path"
        throw new IllegalArgumentException(errorMessage)
    }
    implicit val sc = getSparkContext(master, name)
    implicit val sqlContext = SQLContext.getOrCreate(sc)
    aggregate(sqlContext, datadir)
    sc.stop()
  }

  def aggregate(implicit sqlContext: SQLContext, datadir: String): Unit = {
    val appVectors = aggregateToAppVectorsWithBroadcastMap
    writeToHDFS(appVectors)
  }

  case class CCAppsSet(userId: Long, time: Long, apps: Set[Long])

  def codeApps(apps: Array[String])(implicit lookupTable: Broadcast[Map[String, Long]]): Array[Int] =
     apps.flatMap(lookupTable.value.get(_)).map(_.toInt)

  def codeAppsToSet(apps: Array[String])(implicit lookupTable: Broadcast[Map[String, Long]]):Set[Int] =
    codeApps(apps).toSet


  //Trying vector version
  def ones(n:Int):Array[Double] = {
     Array.fill(n)(1.0)
  }

  def codeAppsToSparseVector(apps: Array[String], fillValue: Int = 1)
                            (implicit appsListSize:Int, lookupTable: Broadcast[Map[String, Long]])
                  :Vector =
    Vectors.sparse(appsListSize, codeApps(apps), ones(apps.length))

  case class CCAppsVector(userId: Long, time: Long, appVector:Vector)

  def getAppChanges(appLists:Dataset[CCAppList],
                    appsInstalls:Dataset[CCAppInstall],
                    appsRemoves: Dataset[CCAppRemoval],
                    appUsage:Dataset[CCAggregationEntry])
                   (implicit sqlContext: SQLContext, appsListSize:Int, lookupTable: Broadcast[Map[String, Long]])
        :Dataset[CCAppsVector] = {
    import sqlContext.implicits._
    val appChangesInstalledAtStart = for {
      appListForUser <- appLists
      user = appListForUser.userId
      time = appListForUser.time
      apps = appListForUser.installedApps
    } yield CCAppsVector(user, time, codeAppsToSparseVector(apps.toArray))
    val appsChangesInstalls = for {
      appListForUser <- appsInstalls
      user = appListForUser.userId
      time = appListForUser.time
      app = appListForUser.packageName
      appsVector = codeAppsToSparseVector(Array(app))
    } yield CCAppsVector(user, time, appsVector)
    val appsChangesRemovals = for {
      appListForUser <- appsRemoves
      user = appListForUser.userId
      time = appListForUser.time
      app = appListForUser.packageName
      appsVector = codeAppsToSparseVector(Array(app), -1)
    } yield CCAppsVector(user, time, appsVector)
    val dailyDummies = for {
      usageEntry <- appUsage.groupBy(e => (e.userId, e.time)).keys
      user = usageEntry._1
      time = usageEntry._2
    } yield CCAppsVector(user, time, createDummyVector)
    val appChanges = appChangesInstalledAtStart
                            .union(appsChangesInstalls)
                            .union(appsChangesRemovals)
                            .union(dailyDummies)
    appChanges
  }

  def getAppDictionary(allApps:Dataset[String]) =
    allApps.rdd.zipWithIndex().collect().toMap



  def aggregateToAppVectorsWithBroadcastMap(implicit sqlContext: SQLContext, datadir: String):Dataset[CCAppsVector] = {
    import sqlContext.implicits._
    val appLists:Dataset[CCAppList] = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_APP_LIST).as[CCAppList]
    val appsInstalls:Dataset[CCAppInstall] = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_APP_INSTALL).as[CCAppInstall]
    val appsRemoves:Dataset[CCAppRemoval] = ParquetIO.readEventTypeToDF(sqlContext, datadir, TYPE_APP_REMOVAL).as[CCAppRemoval]

    val appUsage:Dataset[CCAggregationEntry] = ParquetIO.readAggrTypeToDataset(sqlContext, datadir, AggregationType.AppTotalDuration, Granularity.Daily)
    appUsage.groupBy()
    val allApps:Dataset[String] = appLists.flatMap(_.installedApps)
      .union(appsRemoves.select($"packageName".as[String]))
      .union(appsInstalls.select($"packageName".as[String]))
      .distinct
    implicit val appsListSize:Int = allApps.count().toInt
    implicit val lookupTable:Broadcast[Map[String, Long]] = sqlContext.sparkContext.broadcast(getAppDictionary(allApps))
    val appChanges = getAppChanges(appLists, appsInstalls, appsRemoves, appUsage)
    val appVectors = sumAppVectors(appChanges)
    appVectors
  }

  def writeToHDFS(appsVector: Dataset[CCAppsVector])
                 (implicit sqlContext: SQLContext, datadir: String) = {
    ParquetIO.writeAppVectorDataset(datadir, "AppsInstalledVector", Granularity.Daily, appsVector)
  }

  def createDummyVector(implicit n:Int):Vector = {
    Vectors.sparse(n, Array(), Array())
  }

  def generateDailyAppVector(appUsage: Dataset[CCAggregationEntry])
                            (implicit sqlContext: SQLContext, appsListSize:Int, lookupTable: Broadcast[Map[String, Long]])
  :Dataset[CCAppsVector] = {
    import sqlContext.implicits._
    appUsage.filter(_.value > 0).map(x => CCAppsVector(x.userId, x.time, createDummyVector(appsListSize)))
  }

  def addAppVectors(vec1: Vector, vec2: Vector):Vector = {
    (vec1.asBreeze + vec2.asBreeze).fromBreeze
  }

  def findLowerBound(ar:Array[Int], value:Int): Int = {
    var b = ar.length - 1
    var a = 0
    while (a < b) {
      val i  = (a + b) >> 2
      if (ar(i) < value) a = i + 1
      else b = i
    }
    a
  }


  def addAppChange(vec1: Vector, vec2: Vector):Vector = {
    val sVec1 = vec1.toSparse
    val sVec2 = vec2.toSparse
    val indices = sVec1.toSparse.indices
    val values = sVec1.values
    val len = indices.length
    val n = sVec1.size
    if (! sVec2.indices.isEmpty) {
      val updatedIndex = sVec2.indices(0)
      val isInstall = sVec2.values(0) > 0
      val i = findLowerBound(indices, updatedIndex)
      if (isInstall && (indices(i) != updatedIndex))
          Vectors.sparse(n,
          Array.concat(indices.slice(0, i - 1), Array(updatedIndex), indices.slice(i, len)),
          ones(len + 1))
      else if (!isInstall && indices(i) == updatedIndex)
          Vectors.sparse(n,
            Array.concat(indices.slice(0, i - 1), indices.slice(i + 1, len)),
            ones(len - 1))
    }
    vec1
  }

  def appVectorsSum(appsVector1: CCAppsVector, appsVector2: CCAppsVector): CCAppsVector = {
    val v = addAppChange(appsVector1.appVector, appsVector2.appVector)
    appsVector2.copy(appVector = v)
  }

  def sortAndSumAppVectors(user: Long, changes: Iterator[CCAppsVector])(implicit appsListSize:Int):Iterable[CCAppsVector] = {
    changes.toArray
      .sortBy(_.time)
      .scanLeft(CCAppsVector(user, 0, createDummyVector))(appVectorsSum)
  }

  def sumAppVectors(appChanges: Dataset[CCAppsVector])
                   (implicit sqlContext: SQLContext,
                    appsListSize:Int,
                    lookupTable: Broadcast[Map[String, Long]]) ={
    import sqlContext.implicits._
    appChanges.groupBy(_.userId).flatMapGroups(sortAndSumAppVectors)
  }


}

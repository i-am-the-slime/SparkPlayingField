package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}
import org.menthal.aggregations.SleepAggregations.UserWindow
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.{EventType, Granularity}
import org.menthal.model.events.Implicits._
import org.menthal.model.Granularity._
import org.menthal.model.events.{Localisation, CCLocalisation}
import org.menthal.spark.SparkHelper._
import math._

/**
 * Created by konrad on 21/05/15.
 */
object DistanceCoveredAggregation {

  val name: String = "LocationChangeAggregation"
  def main(args: Array[String]) {
    val (master, datadir) = args match {
      case Array(m, d) => (m,d)
      case _ =>
        val errorMessage = "First argument is master, second input/output path"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, name)
    aggregate(sc, datadir)
    //parquetToPhoneSessions(sc, datadir)
    sc.stop()
  }


  def aggregate(sc: SparkContext, datadir: String): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val locations = ParquetIO.readEventType(sc, datadir, EventType.TYPE_LOCALISATION).map(toCCLocalisation)

    val furthestPoints = furthersPointsEachDay(locations)
    furthestPoints.toDF().write.parquet(datadir + "/dailyRadius")
    val dailyPath = calculateDailyPath(locations)
    dailyPath.toDF().write.parquet(datadir + "/pathCovered")



  }



  case class CCDailyRadius(userId: Long, day: Long, lat1:Double, lng1:Double, lat2:Double, lng2:Double, dist: Double)
  case class CCPathCovered(userId: Long, day: Long, points: Int, distance: Double)



  def calculateDailyPath(locations: RDD[CCLocalisation]):RDD[CCPathCovered] = {
    val pointsByUserDays = for {
      CCLocalisation(_, userId, time, _, accuracy, lng, lat) <- locations
      if accuracy > 0
    } yield ((userId, roundTimestamp(time, Daily)), ((lat, lng), time))
    val dailyDistByUserDays = pointsByUserDays.groupByKey.mapValues(sortAndCalulatePathLenghtGeo)
    for {
      ((userId, day), (n, dist)) <- dailyDistByUserDays
    } yield CCPathCovered(userId, day, n, dist)
  }

  def updateLastPointAndDist(dist: (Coordinates,Coordinates) => Double)
                  (acc:(PointWithTime, Double), currentPointWithTime:PointWithTime) = {
    val (lastPoint:Coordinates, _)  = acc._1
    val distanceSoFar:Double = acc._2
    val currentPoint = currentPointWithTime._1
    val d = distanceSoFar + dist(lastPoint, currentPoint)
    (currentPointWithTime,d)
  }

  def sortAndCalulatePathLenght(dist: (Coordinates,Coordinates) => Double)
                               (pointsWithTime : Iterable[PointWithTime]):(Int,Double) = {
    val pointsSorted = pointsWithTime.toArray.sortBy(_._2)
    val n = pointsSorted.length
    val start = pointsSorted.head
    val cumDist:Double = 0
    val (last:PointWithTime, d:Double) = pointsSorted.foldLeft((start, cumDist))(updateLastPointAndDist(dist) _ )
    (n, d)
  }

  def sortAndCalulatePathLenghtGeo = sortAndCalulatePathLenght(haversineDist)  _

  def furthersPointsEachDay(locations: RDD[CCLocalisation]):RDD[CCDailyRadius] = {
    val pointsByUserDays = for {
      CCLocalisation(_, userId, time, _, accuracy, lng, lat) <- locations
      if accuracy > 0
    } yield ((userId, roundTimestamp(time, Daily)), (lat, lng))

    val furthestPointsByUserDays = pointsByUserDays.groupByKey().mapValues(findFurthestPointGeo)
    for {
      ((userId, day), (point1, point2, dist)) <- furthestPointsByUserDays
      (lat1, lng1) = point1
      (lat2, lng2) = point2
    } yield CCDailyRadius(userId, day, lat1, lng1, lat2, lng2, dist)
  }

  type PointWithTime = (Coordinates, Long)
  type Coordinates = (Double, Double)
  type PointsWithDist = (Coordinates, Coordinates, Double)


  def findFurthestPoints(dist: (Coordinates,Coordinates) => Double)(points: Iterable[(Double, Double)]):PointsWithDist = {
    val pairsWithDist = for {
                     x <- points
                     y <- points
                     d = dist(x, y)
                     //d = haversineDist(x, y)
    } yield (x, y, d)
    pairsWithDist.maxBy { case (x, y, d) => d}
  }



  def euclidianDist(point1: Coordinates, point2: Coordinates): Double = {
    val (x1, y1) = point1
    val (x2, y2) = point1
    sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))
  }

  val R = 6372.8

  def haversineDist(point1: Coordinates, point2: Coordinates): Double={
    val (lat1, lon1) = point1
    val (lat2, lon2) = point2
    val dLat=(lat2 - lat1).toRadians
    val dLon=(lon2 - lon1).toRadians
    val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c
  }

  def findFurthestPointGeo = findFurthestPoints(haversineDist) _


}

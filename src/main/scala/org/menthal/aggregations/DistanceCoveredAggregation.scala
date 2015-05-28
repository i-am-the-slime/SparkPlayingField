package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
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
    val points = farthersPointsEachDay(locations)
    points.toDF().saveAsParquetFile(datadir + "/distanceCovered")
  }

  case class CCPathCovered(userId: Long, day: Long, lat1:Double, lng1:Double, lat2:Double, lng2:Double, dist: Double)

  def farthersPointsEachDay(locations: RDD[CCLocalisation]):RDD[CCPathCovered] = {
    val pointsByUserDays = for {
      CCLocalisation(_, userId, time, _, accuracy, lng, lat) <- locations
      if accuracy > 0
    } yield ((userId, roundTimestamp(time, Daily)), (lat, lng))

    val pointsGroupedByUserDays = pointsByUserDays.groupByKey()
    val furthestPointsByUserDays = pointsGroupedByUserDays.mapValues(findFurthestPointGeo)
    for {
      ((userId, day), (point1, point2, dist)) <- furthestPointsByUserDays
      (lat1, lng1) = point1
      (lat2, lng2) = point2
    } yield CCPathCovered(userId, day, lat1, lng1, lat2, lng2, dist)
  }

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

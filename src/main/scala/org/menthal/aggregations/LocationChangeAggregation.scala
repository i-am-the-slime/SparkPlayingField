package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.menthal.model.events._
import org.menthal.spark.SparkHelper._

/**
 * Created by konrad on 20/05/15.
 */
object LocationChangeAggregation {

  case class CCLocationChangeFragment(userId: Long, locationChangeTime: Long,
                                       locationCluster:LocationCluster, time: Long,
                                      duration: Long, app: String)

  val name: String = "LocationChangeAggregation"

  def main(args: Array[String]) {
    val (master, datadir) = args match {
      case Array(m, d) => (m,d)
      case _ =>
        val errorMessage = "First argument is master, second input/output path"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, name)
    //aggregate(sc, datadir)
    parquetToLocationChangeFragment(sc, datadir)
    sc.stop()
  }


  def parquetToLocationChangeFragment(sc: SparkContext, datadir: String): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val phoneSessions = sqlContext.parquetFile(datadir + "/phoneSessions")
    val sessions:RDD[MenthalEvent] = for {
      row <- phoneSessions
      userId = row.getLong(0)
      time = row.getLong(2)
      duration = row.getLong(3)
      name = row.getString(4)
    } yield CCAppSession(userId, time, duration, name)
    val clusters = sqlContext.parquetFile(datadir + "/locationClusters")
    clusters.registerTempTable("clusters")
    val rawLocations = sqlContext.parquetFile(datadir + "/localisation")
    rawLocations.registerTempTable("locations")
    val query = "SELECT l.userId, l.time, l.signalType, l.accuracy, l.longitude, l.latitude, c.cluster" +
      " FROM locations l JOIN clusters c WHERE c.id = l.id and l.accuracy != 0"
    val locationWithCluster = sqlContext.sql(query)
    val locations:RDD[MenthalEvent] = for {
      row <- locationWithCluster
      userId = row.getLong(0)
      time = row.getLong(1)
      signalType = row.getString(2)
      accuracy = row.getFloat(3)
      lng = row.getDouble(4)
      lat = row.getDouble(5)
      clusterId = row.getLong(6)
    } yield CCLocalisation(clusterId, userId, time, signalType , accuracy, lng, lat)
    val processedEvents:RDD[MenthalEvent] = sessions ++ locations
    val locationChangeFragments = eventsToLocationChangeFragments(processedEvents)
    locationChangeFragments.toDF().saveAsParquetFile(datadir + "/locationSessions")
  }


  type LocationCluster = Long
  type LocationState = (Long, Long)
  type LocationSessionAccumulator = (LocationState, List[CCLocationChangeFragment])

  def locationSessionsFold(acc: LocationSessionAccumulator, event: MenthalEvent): LocationSessionAccumulator = {
    val (state, sessions) = acc
    val (currentLocation, locationStartTime) = state

    if (currentLocation != -1) event match {
      case CCAppSession(userId, time, duration, name) =>
        (state, CCLocationChangeFragment(userId,locationStartTime, currentLocation,
                                        time, duration, name) :: sessions)

      case CCLocalisation(clusterId, _, _, _, 0, _,_) =>  (state, sessions)
      case CCLocalisation(clusterId, userId, time, _, accuracy, _,_) =>  ((clusterId.toInt, time), sessions)

      case _ => (state, sessions)
    } else event match {
      case CCLocalisation(clusterId, _, _, _, 0, _,_) =>  (state, sessions)
      case CCLocalisation(clusterId, userId, time, _, accuracy, _,_) =>  ((clusterId.toInt, time), sessions)

      case _ => (state, sessions)
    }
  }

  val startingState: LocationState = (-1, 0)
  val foldEmptyAcc: LocationSessionAccumulator = (startingState, Nil)

  def transformToLocationChangeFragments(events: Iterable[_ <: MenthalEvent]): List[CCLocationChangeFragment] = {
    val sortedEvents = events.toList.sortBy(_.time)
    val (_, phoneSessions) = sortedEvents.foldLeft(foldEmptyAcc)(locationSessionsFold)
    phoneSessions
  }

  def eventsToLocationChangeFragments(events: RDD[_ <: MenthalEvent]): RDD[CCLocationChangeFragment] = {
    events.map { e => (e.userId, e) }
      .groupByKey()
      .values
      .flatMap(transformToLocationChangeFragments)
  }
}

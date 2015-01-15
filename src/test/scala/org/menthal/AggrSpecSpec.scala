package org.menthal

import org.apache.spark.SparkContext
import org.menthal.aggregations.AggrSpec
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.{EventType, Granularity}
import org.menthal.model.Granularity._
import org.menthal.model.events.Implicits._
import org.menthal.model.events.{CCAggregationEntry, AggregationEntry, AppSession, AppInstall}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, Matchers, FlatSpec}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.reflect.io.File
import scala.util.Try

/**
 * Created by konrad on 13.01.15.
 */
class AggrSpecSpec extends FlatSpec with GeneratorDrivenPropertyChecks with Matchers with BeforeAndAfterEach with BeforeAndAfter {


  @transient var sc: SparkContext = _
  val basePath = "src/test/resources/"
  val datadir = basePath + "Agggggrrrr"
  val timePeriod = Granularity.Hourly

  override def beforeEach() {
    sc = SparkTestHelper.localSparkContext
  }

  override def afterEach() {

   // Try(File(datadir).deleteRecursively())
    sc.stop()
    sc = null
  }


  "aggregateSuiteForGranularity should read data from postgres and then aggregate in package" should "be possible" in {
    forAll(Generators.listAppSession) { sessions ⇒
      Try(File(datadir).deleteRecursively())

      val sessionsRdd = sc.parallelize(sessions).map(_.toAvro)
      ParquetIO.writeEventType(sc, datadir, EventType.TYPE_APP_SESSION, sessionsRdd)
      //AggrSpec.aggregateCountFromParquet(toCCAppSession)(datadir, EventType.TYPE_APP_SESSION, timePeriod, "appStarts", sc)
//      val result = ParquetIO.readAggrType(sc, datadir, "appStarts", timePeriod).map(toCCAggregationEntry).collect()
////      val result = ParquetIO.read[AggregationEntry](sc, datadir + "/" + "appStarts" + Granularity.asString(timePeriod)).map(toCCAggregationEntry).collect()
//
//      val keyVals = Generators.splitToBucketsWithCount(timePeriod, sessions)
//      val expected = for (((user, pn, time), count) <- keyVals)
//      yield CCAggregationEntry(user, time, timePeriod, pn, count)
//
//      result.toSet() equals expected.toSet()

    }
  }
}
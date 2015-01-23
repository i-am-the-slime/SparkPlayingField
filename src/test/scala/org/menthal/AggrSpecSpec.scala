package org.menthal

import org.apache.spark.SparkContext
import org.menthal.aggregations.tools.AggrSpec
import org.menthal.aggregations.tools.AggrSpec._
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.EventType._
import org.menthal.model.Granularity.Leaf
import org.menthal.model.{AggregationType, EventType, Granularity}
import org.menthal.model.events.CCAggregationEntry
import org.menthal.model.events.Implicits._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FlatSpec, Matchers}

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

  val granularities = List(Leaf(Granularity.Hourly))
  val simpleAggrSpecs = List(
    AggrSpec(TYPE_APP_SESSION, toCCAppSession _, count(AggregationType.AppTotalCount)))

  "aggregateSuiteForGranularity should read data from postgres and then aggregate in package" should "be possible" in {
    forAll(Generators.nonEmptyListOfAppSessions) { sessions ⇒
      Try(File(datadir).deleteRecursively())
      //beforeEach()
      val sessionsRdd = sc.parallelize(sessions).map(_.toAvro)
        ParquetIO.writeEventType(sc, datadir, EventType.TYPE_APP_SESSION, sessionsRdd)
//        ParquetIO.readEventType(sc, datadir, EventType.TYPE_APP_SESSION)
        AggrSpec.aggregate(sc, datadir, simpleAggrSpecs, granularities)
      val result = ParquetIO.readAggrType(sc, datadir, AggregationType.AppTotalCount, timePeriod).map(toCCAggregationEntry).collect()
      val keyVals = Generators.splitToBucketsWithCount(timePeriod, sessions)
      val expected = for (((user, pn, time), count) <- keyVals)
      yield CCAggregationEntry(user, time, timePeriod, pn, count)
      result.toSet() shouldBe expected.toSet()
    }
  }
}
package org.menthal.aggregations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.menthal.aggregations.tools.SummaryTransformers
import org.menthal.model.implicits.DateImplicits._
import org.menthal.io.parquet.ParquetIO
import org.menthal.model.AggregationType._
import org.menthal.model.Granularity
import org.menthal.model.Granularity._
import org.menthal.model.events.Implicits._
import org.menthal.model.events.{CCAggregationEntry, Summary}
import com.twitter.algebird.Operators._
import org.menthal.spark.SparkHelper._

/**
 * Created by konrad on 22.01.15.
 */
object SummaryAggregation {
  val name = "SummaryAggregation"
  def main(args: Array[String]) = {
    val (master, datadir) = args match {
      case Array(m, d) =>
        (m,d)
      case _ =>
        val errorMessage = "First argument is master, second datadir path"
        throw new IllegalArgumentException(errorMessage)
    }
    val sc = getSparkContext(master, name)
    aggregate(sc, datadir)
    sc.stop()
  }

  def aggregate(sc: SparkContext, datadir: String) = {
    def aggregateAggregationsToParquet(subAggregates: RDD[Summary], granularity: TimePeriod) :RDD[Summary] = {
      val summaries = aggregateSummaries(subAggregates, granularity)
      ParquetIO.writeSummary(sc, datadir, granularity, summaries)
      summaries
    }
    for (granularityTree ← Granularity.fullGranularitiesForest) {
      val summaries = createSummaryAggregationFromParquet(sc, datadir, granularityTree.a)
      val treeTraverse = granularityTree.traverseTree(summaries) _
      treeTraverse(aggregateAggregationsToParquet)
    }
  }

  def aggregateSummaries(summaries: RDD[Summary], granularity: TimePeriod): RDD[Summary] = {
      val buckets = for {
        summary <- summaries
        time = longToDate(summary.getTime)
        user = summary.getUserId
        timeBucket = Granularity.roundTimeFloor(time, granularity)
        values = SummaryTransformers.summaryValues(summary)
      } yield ((user, timeBucket), values)
      buckets reduceByKey (_ + _) map { case ((user, time), values) =>
        SummaryTransformers.createSummary(user, time, granularity.toInt, values)
      }
    }


  def createSummaryAggregationFromParquet(sc: SparkContext, datadir: String, granularity: TimePeriod): RDD[Summary] = {

      type AggTuple = ((Long, Long, Long), Long)
      def transformAggregationsInParquet(fn: RDD[CCAggregationEntry] => RDD[AggTuple])
                                        (inputAggrNames: List[String], name: String, granularity: TimePeriod): RDD[CCAggregationEntry] = {
        val inputRDDs = inputAggrNames.map(n => ParquetIO.readAggrType(sc, datadir, n, granularity).map(toCCAggregationEntry))
        val inputRDD = inputRDDs.reduce(_ ++ _)
        fn(inputRDD).map { case ((u, t, g), v) => CCAggregationEntry(u, t, g, name, v)}
      }

      def sumKeys(input: RDD[CCAggregationEntry]): RDD[AggTuple] = {
        input.map(e => ((e.userId, e.time, e.granularity), e.value)).reduceByKey(_ + _)
      }
      def countKeys(input: RDD[CCAggregationEntry]): RDD[AggTuple] = {
        input.map(e => ((e.userId, e.time, e.granularity), e.key)).distinct().mapValues(_ ⇒ 1L).reduceByKey(_ + _)
      }

      def sumValues = transformAggregationsInParquet(sumKeys) _

      def countDistinctKeys = transformAggregationsInParquet(countKeys) _

      //SMS
      val smsInCount: RDD[CCAggregationEntry] = sumValues(List(SmsInCount), SmsInCount, granularity)
      val smsOutCount: RDD[CCAggregationEntry] = sumValues(List(SmsOutCount), SmsOutCount, granularity)
      val smsTotalCount: RDD[CCAggregationEntry] = sumValues(List(SmsInCount, SmsOutCount), SmsTotalCount, granularity)
      val smsInParticipants: RDD[CCAggregationEntry] = countDistinctKeys(List(SmsInCount), SmsInParticipants, granularity)
      val smsOutParticipants: RDD[CCAggregationEntry] = countDistinctKeys(List(SmsOutCount), SmsOutParticipants, granularity)
      val smsTotalParticipants: RDD[CCAggregationEntry] = countDistinctKeys(List(SmsInCount, SmsOutCount), SmsTotalParticipants, granularity)
      val smsInLength: RDD[CCAggregationEntry] = sumValues(List(SmsInLength), SmsInLength, granularity)
      val smsOutLength: RDD[CCAggregationEntry] = sumValues(List(SmsOutLength), SmsOutLength, granularity)
      //Calls
      val callInCount: RDD[CCAggregationEntry] = sumValues(List(CallInCount), CallInCount, granularity)
      val callMissCount: RDD[CCAggregationEntry] = sumValues(List(CallMissCount), CallMissCount, granularity)
      val callOutCount: RDD[CCAggregationEntry] = sumValues(List(CallOutCount), CallOutCount, granularity)
      val callTotalCount: RDD[CCAggregationEntry] = sumValues(List(CallInCount, CallOutCount), CallTotalCount, granularity)
      val callInParticipants: RDD[CCAggregationEntry] = countDistinctKeys(List(CallInCount), CallInParticipants, granularity)
      val callOutParticipants: RDD[CCAggregationEntry] = countDistinctKeys(List(CallOutCount), CallOutParticipants, granularity)
      val callTotalParticipants: RDD[CCAggregationEntry] = countDistinctKeys(List(CallInCount, CallOutCount), CallTotalParticipants, granularity)
      val callInDuration: RDD[CCAggregationEntry] = sumValues(List(CallInDuration), CallInDuration, granularity)
      val callOutDuration: RDD[CCAggregationEntry] = sumValues(List(CallOutDuration), CallOutDuration, granularity)
      val callTotalDuration: RDD[CCAggregationEntry] = sumValues(List(CallInDuration, CallOutDuration), CallTotalDuration, granularity)
      //Apps
      val appTotalCountUnique: RDD[CCAggregationEntry] = countDistinctKeys(List(AppTotalCount), AppTotalCountUnique, granularity)
      val appTotalCount: RDD[CCAggregationEntry] = sumValues(List(AppTotalCount), AppTotalCount, granularity)
      val appTotalDuration: RDD[CCAggregationEntry] = sumValues(List(AppTotalDuration), AppTotalDuration, granularity)
      //Screen
      val screenUnlocksCount: RDD[CCAggregationEntry] = sumValues(List(ScreenUnlocksCount), ScreenUnlocksCount, granularity)
      val screenOffCount: RDD[CCAggregationEntry] = sumValues(List(ScreenOffCount), ScreenOffCount, granularity)
      val screenOnCount: RDD[CCAggregationEntry] = sumValues(List(ScreenOnCount), ScreenOnCount, granularity)
      //Phone
      val phoneShutdownsCount: RDD[CCAggregationEntry] = sumValues(List(PhoneShutdownsCount), PhoneShutdownsCount, granularity)
      val phoneBootsCount: RDD[CCAggregationEntry] = sumValues(List(PhoneBootsCount), PhoneBootsCount, granularity)

      val all = smsInCount ++ smsOutCount ++ smsTotalCount ++
        smsInParticipants ++ smsOutParticipants ++ smsTotalParticipants ++
        smsInLength ++ smsOutLength ++
        callInCount ++ callMissCount ++ callOutCount ++ callTotalCount ++
        callInParticipants ++ callOutParticipants ++ callTotalParticipants ++
        callInDuration ++ callOutDuration ++ callTotalDuration ++
        appTotalCountUnique ++ appTotalCount ++ appTotalDuration ++
        screenUnlocksCount ++ screenOffCount ++ screenOnCount ++
        phoneShutdownsCount ++ phoneBootsCount

      all.groupBy(a => (a.userId, a.time, a.granularity))
        .mapValues(_.map(e => (e.key, e.value)).toMap.withDefaultValue(0L))
        .map { case ((u, t, g), m) => SummaryTransformers.createSummary(u, t, g, m)}
    }
  }


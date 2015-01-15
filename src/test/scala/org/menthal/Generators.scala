package org.menthal

import org.joda.time.DateTime
import org.menthal.aggregations.tools.EventTransformers
import org.menthal.model.Granularity
import org.menthal.model.Granularity.TimePeriod
import org.menthal.model.events.CCAppSession
import org.menthal.model.implicits.DateImplicits._
import org.scalacheck.Gen

/**
 * Created by konrad on 13.01.15.
 */
object Generators {

  val appSession:Gen[CCAppSession] = for {
    userId ← Gen.choose(0L, 5L)
    timeOffsetInSeconds ← Gen.choose(0, 203902)
    time = DateTime.now().minusDays(100).plusSeconds(timeOffsetInSeconds).getMillis
    duration ← Gen.choose(0L, 100000L)
    packageName ← Gen.oneOf("com.less.offensive", "org.whatsapp", "org.menthal", "pl.konrad")
  } yield CCAppSession(userId, time, duration, packageName)

  val listAppSession:Gen[List[CCAppSession]] = Gen.listOf(appSession)

  def splitToBucketsWithFun(getVal: CCAppSession => Long)
                           (timePeriod:TimePeriod, sessions:List[CCAppSession])
                           :List[((Long,String, Long),Long)] = {
    for {
      session ← sessions
      split ← EventTransformers.splitEventByRoundedTime(session, timePeriod)
      userId = session.userId
      pn = session.packageName
      bucket = Granularity.roundTimeFloor(split.time, timePeriod)
    } yield ((userId, pn, dateToLong(bucket)), getVal(session))
  }

  def splitToBucketsWithCount = splitToBucketsWithFun {_ => 1} _
  def splitToBucketsWithDuration = splitToBucketsWithFun {s => s.duration} _




}

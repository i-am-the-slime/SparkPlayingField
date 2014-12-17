package org.menthal.io.serialization

import com.twitter.bijection.avro.SpecificAvroCodecs
import com.twitter.chill.InjectiveSerializer
import org.apache.avro.specific.SpecificRecordBase
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import org.menthal.model.events._

object AvroSerializer {
  def asAvroSerializer[A <: SpecificRecordBase : Manifest] = {
    implicit val inj = SpecificAvroCodecs.toBinary[A]
    InjectiveSerializer.asKryo
  }
}
import AvroSerializer._

class MenthalKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[AccessibilityServiceUpdate], asAvroSerializer[AccessibilityServiceUpdate])
    kryo.register(classOf[AggregationEntry], asAvroSerializer[AggregationEntry])
    kryo.register(classOf[AppInstall], asAvroSerializer[AppInstall])
    kryo.register(classOf[AppList], asAvroSerializer[AppList])
    kryo.register(classOf[AppRemoval], asAvroSerializer[AppRemoval])
    kryo.register(classOf[AppUpgrade], asAvroSerializer[AppUpgrade])
    kryo.register(classOf[CallMissed], asAvroSerializer[CallMissed])
    kryo.register(classOf[CallOutgoing], asAvroSerializer[CallOutgoing])
    kryo.register(classOf[CallReceived], asAvroSerializer[CallReceived])
    kryo.register(classOf[DeviceFeatures], asAvroSerializer[DeviceFeatures])
    kryo.register(classOf[DreamingStarted], asAvroSerializer[DreamingStarted])
    kryo.register(classOf[DreamingStopped], asAvroSerializer[DreamingStopped])
    kryo.register(classOf[Localisation], asAvroSerializer[Localisation])
    kryo.register(classOf[MenthalAppEvent], asAvroSerializer[MenthalAppEvent])
    kryo.register(classOf[Mood], asAvroSerializer[Mood])
    kryo.register(classOf[PhoneBoot], asAvroSerializer[PhoneBoot])
    kryo.register(classOf[PhoneShutdown], asAvroSerializer[PhoneShutdown])
    kryo.register(classOf[Questionnaire], asAvroSerializer[Questionnaire])
    kryo.register(classOf[ScreenOff], asAvroSerializer[ScreenOff])
    kryo.register(classOf[ScreenOn], asAvroSerializer[ScreenOn])
    kryo.register(classOf[ScreenUnlock], asAvroSerializer[ScreenUnlock])
    kryo.register(classOf[SmsReceived], asAvroSerializer[SmsReceived])
    kryo.register(classOf[SmsSent], asAvroSerializer[SmsSent])
    kryo.register(classOf[TimeZone], asAvroSerializer[TimeZone])
    kryo.register(classOf[TrafficData], asAvroSerializer[TrafficData])
    kryo.register(classOf[Unknown], asAvroSerializer[Unknown])
    kryo.register(classOf[WhatsAppReceived], asAvroSerializer[WhatsAppReceived])
    kryo.register(classOf[WhatsAppSent], asAvroSerializer[WhatsAppSent])
    kryo.register(classOf[WindowStateChanged], asAvroSerializer[WindowStateChanged])
  }
}

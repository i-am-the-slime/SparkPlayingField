package org.menthal.model.serialization

import com.twitter.bijection.avro.SpecificAvroCodecs
import com.twitter.chill.{MeatLocker, InjectiveSerializer}
import org.apache.spark.rdd.RDD
import org.menthal.model.events._
import org.objenesis.strategy.StdInstantiatorStrategy

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.reflect.ClassTag

import org.apache.avro.io.{BinaryEncoder, EncoderFactory, DecoderFactory, BinaryDecoder}
import org.apache.avro.specific.{SpecificRecordBase, SpecificDatumWriter, SpecificDatumReader, SpecificRecord}

import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.esotericsoftware.kryo.io.{Output, Input}

import it.unimi.dsi.fastutil.io.{FastByteArrayOutputStream, FastByteArrayInputStream}

import org.apache.spark.serializer.KryoRegistrator

object AvroSerializer {
  def asAvroSerializer[A <: SpecificRecordBase : Manifest] = {
    implicit val inj = SpecificAvroCodecs.toBinary[A]
    InjectiveSerializer.asKryo
  }
}
import AvroSerializer._

class MenthalKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[AppInstall], asAvroSerializer[AppInstall])
    kryo.register(classOf[AppRemoval], asAvroSerializer[AppRemoval])
    kryo.register(classOf[CallMissed], asAvroSerializer[CallMissed])
    kryo.register(classOf[CallOutgoing], asAvroSerializer[CallOutgoing])
    kryo.register(classOf[CallReceived], asAvroSerializer[CallReceived])
    kryo.register(classOf[DreamingStarted], asAvroSerializer[DreamingStarted])
    kryo.register(classOf[DreamingStopped], asAvroSerializer[DreamingStopped])
    kryo.register(classOf[Localisation], asAvroSerializer[Localisation])
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
    kryo.register(classOf[WhatsAppReceived], asAvroSerializer[WhatsAppReceived])
    kryo.register(classOf[WhatsAppSent], asAvroSerializer[WhatsAppSent])
    kryo.register(classOf[WindowStateChanged], asAvroSerializer[WindowStateChanged])
  }
}

//André Schumacher
//case class InputStreamWithDecoder(size: Int) {
//  val buffer = new Array[Byte](size)
//  val stream = new FastByteArrayInputStream(buffer)
//  val decoder = DecoderFactory.get().directBinaryDecoder(stream, null.asInstanceOf[BinaryDecoder])
//}
//
//// NOTE: This class is not thread-safe; however, Spark guarantees that only a single thread
//// will access it.
//class AvroSerializer[T <: SpecificRecord](implicit tag: ClassTag[T]) extends Serializer[T] {
//  val reader = new SpecificDatumReader[T](tag.runtimeClass.asInstanceOf[Class[T]])
//  val writer = new SpecificDatumWriter[T](tag.runtimeClass.asInstanceOf[Class[T]])
//  var in = InputStreamWithDecoder(1024)
//  val outstream = new FastByteArrayOutputStream()
//  val encoder = EncoderFactory.get().directBinaryEncoder(outstream, null.asInstanceOf[BinaryEncoder])
//
//  setAcceptsNull(false)
//
//  def write(kryo: Kryo, kryoOut: Output, record: T) = {
//    outstream.reset()
//    writer.write(record, encoder)
//    kryoOut.writeInt(outstream.array.length, true)
//    kryoOut.write(outstream.array)
//  }
//
//  def read(kryo: Kryo, kryoIn: Input, klazz: Class[T]): T = this.synchronized {
//    val len = kryoIn.readInt(true)
//    if (len > in.size) {
//      in = InputStreamWithDecoder(len + 1024)
//    }
//    in.stream.reset()
//    // Read Kryo bytes into input buffer
//    kryoIn.readBytes(in.buffer, 0, len)
//    // Read the Avro object from the buffer
//    reader.read(null.asInstanceOf[T], in.decoder)
//  }
//}
//
//class MenthalKryoRegistrator extends KryoRegistrator {
//  override def registerClasses(kryo: Kryo) {
//    kryo.register(classOf[AppInstall], new AvroSerializer[AppInstall]())
//    kryo.register(classOf[AppRemoval], new AvroSerializer[AppRemoval]())
//    kryo.register(classOf[CallMissed], new AvroSerializer[CallMissed]())
//    kryo.register(classOf[CallOutgoing], new AvroSerializer[CallOutgoing]())
//    kryo.register(classOf[CallReceived], new AvroSerializer[CallReceived]())
//    kryo.register(classOf[DreamingStarted], new AvroSerializer[DreamingStarted]())
//    kryo.register(classOf[DreamingStopped], new AvroSerializer[DreamingStopped]())
//    kryo.register(classOf[Localisation], new AvroSerializer[Localisation]())
//    kryo.register(classOf[Mood], new AvroSerializer[Mood]())
//    kryo.register(classOf[PhoneBoot], new AvroSerializer[PhoneBoot]())
//    kryo.register(classOf[PhoneShutdown], new AvroSerializer[PhoneShutdown]())
//    kryo.register(classOf[Questionnaire], new AvroSerializer[Questionnaire]())
//    kryo.register(classOf[ScreenOff], new AvroSerializer[ScreenOff]())
//    kryo.register(classOf[ScreenOn], new AvroSerializer[ScreenOn]())
//    kryo.register(classOf[ScreenUnlock], new AvroSerializer[ScreenUnlock]())
//    kryo.register(classOf[SmsReceived], new AvroSerializer[SmsReceived]())
//    kryo.register(classOf[SmsSent], new AvroSerializer[SmsSent]())
//    kryo.register(classOf[TimeZone], new AvroSerializer[TimeZone]())
//    kryo.register(classOf[TrafficData], new AvroSerializer[TrafficData]())
//    kryo.register(classOf[WhatsAppReceived], new AvroSerializer[WhatsAppReceived]())
//    kryo.register(classOf[WhatsAppSent], new AvroSerializer[WhatsAppSent]())
//    kryo.register(classOf[WindowStateChanged], new AvroSerializer[WindowStateChanged]())
//  }

package org.menthal.model.serialization

import org.menthal.model.jevents._

import scala.reflect.ClassTag

import org.apache.avro.io.{BinaryEncoder, EncoderFactory, DecoderFactory, BinaryDecoder}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecord}

import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.esotericsoftware.kryo.io.{Output, Input}

import it.unimi.dsi.fastutil.io.{FastByteArrayOutputStream, FastByteArrayInputStream}

import org.apache.spark.serializer.KryoRegistrator


import scala.reflect.ClassTag

case class InputStreamWithDecoder(size: Int) {
  val buffer = new Array[Byte](size)
  val stream = new FastByteArrayInputStream(buffer)
  val decoder = DecoderFactory.get().directBinaryDecoder(stream, null.asInstanceOf[BinaryDecoder])
}

class AvroSerializer[T <: SpecificRecord : ClassManifest] extends Serializer[T] {
  val reader = new SpecificDatumReader[T](classManifest[T].erasure.asInstanceOf[Class[T]])
  val writer = new SpecificDatumWriter[T](classManifest[T].erasure.asInstanceOf[Class[T]])
  var in = InputStreamWithDecoder(1024)
  val outstream = new FastByteArrayOutputStream()
  val encoder = EncoderFactory.get().directBinaryEncoder(outstream, null.asInstanceOf[BinaryEncoder])

  setAcceptsNull(false)

  def write(kryo: Kryo, kryoOut: Output, record: T) = {
    outstream.reset()
    writer.write(record, encoder)
    kryoOut.writeInt(outstream.array.length, true)
    kryoOut.write(outstream.array)
  }

  def read(kryo: Kryo, kryoIn: Input, klazz: Class[T]): T = this.synchronized {
    val len = kryoIn.readInt(true)
    if (len > in.size) {
      in = InputStreamWithDecoder(len + 1024)
    }
    in.stream.reset()
    // Read Kryo bytes into input buffer
    kryoIn.readBytes(in.buffer, 0, len)
    // Read the Avro object from the buffer
    reader.read(null.asInstanceOf[T], in.decoder)
  }
}

class MenthalKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Event], new AvroSerializer[Event])
    kryo.register(classOf[WindowStateChanged], new AvroSerializer[WindowStateChanged])
    kryo.register(classOf[AppInstall], new AvroSerializer[AppInstall])
    kryo.register(classOf[ScreenOff], new AvroSerializer[ScreenOff])
    kryo.register(classOf[ScreenOn], new AvroSerializer[ScreenOn])
    kryo.register(classOf[ScreenUnlock], new AvroSerializer[ScreenUnlock])
  }
}

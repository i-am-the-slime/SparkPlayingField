package org.menthal.model.serialization

/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.gensler.scalavro.types.AvroType
import org.apache.avro.Schema
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecord}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, BinaryEncoder, EncoderFactory}
import org.apache.spark.serializer.KryoRegistrator
import org.menthal.model.events.Event
import org.menthal.model.events.EventData.{EventData, WindowStateChanged, ScreenOn}
import scala.reflect.runtime.universe._

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

class AvroSerializer[T : TypeTag] extends Serializer[T] {
  setAcceptsNull(false)

  def write(kryo: Kryo, output: Output, record: T) = {
    System.out.println("FUCK!")
    AvroType[T].io.write(record, output.getOutputStream)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[T]): T = this.synchronized {
    System.out.println("YOU!")
    AvroType[T].io.read(input.getInputStream) match {
      case Success(readResult) => readResult
      case Failure(cause) => null.asInstanceOf[T]
    }
  }
}

class MenthalKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Event], new AvroSerializer[Event])
    kryo.register(classOf[EventData], new AvroSerializer[EventData])
    kryo.register(classOf[WindowStateChanged], new AvroSerializer[WindowStateChanged])
  }
}

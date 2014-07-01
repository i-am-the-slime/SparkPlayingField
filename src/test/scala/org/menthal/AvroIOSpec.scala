package org.menthal

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import com.gensler.scalavro.types.AvroType
import org.joda.time.DateTime
import org.menthal.model.events.Event
import org.menthal.model.events.EventData._
import org.scalatest.{Matchers, FlatSpec}

import scala.reflect.io.File
import scala.util.Success

class AvroIOSpec extends FlatSpec with Matchers {


  ignore should "be serializable to disk in binary format" in {

    val sc = SparkTestHelper.getLocalSparkContext
    val data = List(Event(12, 12, DateTime.now().getMillis, WindowStateChanged("fuck", "you", "asshole")))
    val events = sc.parallelize(data)
//    val mapped = events.map{
//      case e:Event => e.data match {
//        case off:ScreenOff => AvroType[ScreenOff].io.
//      }
//    }
    val path = "./src/test/resources/avro-io-results"
    File(path).deleteIfExists()
    events.saveAsObjectFile(path)
    //rdds.saveAsTextFile("./src/test/resources/avro-io-results")
  }

  ignore should "read and write Events as JSON" in {
    val io = AvroType[Event].io
    val out = new ByteArrayOutputStream

    val event = new Event(12,12,12,ScreenOff())
    io.write(event, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(event))
  }

  it should "read and write ScreenOff as JSON" in {
    val io = AvroType[ScreenOff].io
    val out = new ByteArrayOutputStream

    val screenOff = ScreenOff()
    io.write(screenOff, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(screenOff))
  }

  it should "read and write WindowStateChange as JSON" in {
    val io = AvroType[WindowStateChanged].io
    val out = new ByteArrayOutputStream

    val wsc = WindowStateChanged("hey", "you", "o")
    io.write(wsc, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(wsc))
  }
  it should "read and write union members derived from class hierarchies as JSON" in {
    val classUnion = AvroType[Alpha].io

    val first = Delta()
    val second: Alpha = Gamma(123.45)

    val json1 = classUnion writeJson first
    val json2 = classUnion writeJson second

    classUnion readJson json1 should equal (Success(first))
    classUnion readJson json2 should equal (Success(second))
  }

  ignore should "fuck around" in {
    val stuff:Alpha = Gamma(2.0)

    val io = AvroType[Alpha].io
    val out = new ByteArrayOutputStream()

    io.write(stuff, out)
  }

  ignore should "read and write a Sequence of EventData as JSON" in {
    val x = AvroType[Seq[EventData]]
    val io = x.io

    val out = new ByteArrayOutputStream

    val stuff:Seq[EventData] = Seq(ScreenOff(), WindowStateChanged("","",""))
    io.write(stuff, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(stuff))
  }

  ignore should "read and write java.lang.Bytes as JSON" in {
    val io = com.gensler.scalavro.io.primitive.AvroByteIO

    val out = new ByteArrayOutputStream

    io.write(5.toByte, out)
    io.write(2.toByte, out)

    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(5.toByte))
    io read in should equal (Success(2.toByte))

  }

  ignore should "be deserializable from disk" in {

  }
}

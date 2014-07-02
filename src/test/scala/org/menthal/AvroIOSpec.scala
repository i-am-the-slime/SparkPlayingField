package org.menthal

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import com.gensler.scalavro.types.AvroType
import org.joda.time.DateTime
import org.menthal.model.events._
import org.scalatest.{Matchers, FlatSpec}

import scala.reflect.io.File
import scala.util.Success

class AvroIOSpec extends FlatSpec with Matchers {


  "Avro" should "read and write ScreenOff as binary" in {
    val io = AvroType[ScreenOff].io
    val out = new ByteArrayOutputStream

    val screenOff = ScreenOff()
    io.write(screenOff, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(screenOff))
  }

  it should "read and write WindowStateChange as binary" in {
    val io = AvroType[WindowStateChanged].io
    val out = new ByteArrayOutputStream

    val wsc = WindowStateChanged("hey", "you", "o")
    io.write(wsc, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(wsc))
  }

  it should "read and write a Sequence of EventData as binary" in {
    val x = AvroType[Seq[EventData]]
    val io = x.io

    val out = new ByteArrayOutputStream

    val stuff:Seq[EventData] = Seq(WindowStateChanged("","",""), ScreenOff())
    io.write(stuff, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(stuff))
  }

  it should "read and write a Sequence of Event as binary" in {
    val x = AvroType[Seq[Event]]
    val io = x.io

    val out = new ByteArrayOutputStream

    val stuff:Seq[Event] = Seq(Event(12,12,12, WindowStateChanged("","","")))
    io.write(stuff, out)
    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(stuff))
  }

  "RDDs of Events" should "be serializable to disk in binary format" in {

    val sc = SparkTestHelper.getLocalSparkContext
    val data = Seq(
      Event(12, 12, DateTime.now().getMillis, WindowStateChanged("fuck", "you", "asshole")),
      Event(13, 15, DateTime.now().getMillis, WindowStateChanged("fuck", "jau", "asshole")),
    )
    val events = sc.parallelize(data)
    val path = "./src/test/resources/avro-io-results"
    File(path).deleteIfExists()
    events.saveAsObjectFile(path)
  }

  it should "be deserializable from disk" in {

  }
}

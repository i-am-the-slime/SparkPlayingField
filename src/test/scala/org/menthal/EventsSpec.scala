package org.menthal

import java.io.File
import java.io.File

import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import org.menthal.model.events._
import org.menthal.model.events.adapters.PostgresDump
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import scala.io.Source
import org.joda.time.DateTime
import spray.json._
import org.menthal.model.events.EventData._

import scala.reflect.io.File

/**
 * Created by mark on 04.06.14.
 */
class EventsSpec extends FlatSpec with Matchers with BeforeAndAfterAll{
  "Creating events from the generated code" should "be possible" in {
    val siasd = AppInstall(1,2,3,"appName", "pkgName")
    info("" + siasd)
  }
  "Creating events " should "work." in {
    val eventLines = Source.fromURL(getClass.getResource("/raw_events")).getLines()
    val events = eventLines.map(PostgresDump.tryToParseLineFromDump).toList
    val correct = List(
      Some(Event(79821970, 8828,  DateTime.parse("2014-01-23T21:58:44.752+01").getMillis,ScreenOn())),
      Some(Event(79822117, 22812,  DateTime.parse("2014-01-22T22:44:04.719+01").getMillis,WindowStateChanged("WhatsApp", "com.whatsapp", "[WhatsApp]"))),
      Some(Event(79822206, 23858,  DateTime.parse("2014-01-24T00:53:14.207+01").getMillis,WindowStateChanged("WhatsApp", "com.whatsapp", "ComponentInfo{com.whatsapp/com.whatsapp.ContactInfo}"))),
      Some(Event(79822106, 18261,	 DateTime.parse("2014-01-23T18:05:55.668+01").getMillis, ScreenOff())),
      Some(Event(79822185, 23858,  DateTime.parse("2014-01-24T00:51:06.82+01").getMillis,ScreenUnlock())),
      None,
      Some(Event(79815448, 18930,  DateTime.parse("2014-01-24T00:43:31.724+01").getMillis,DreamingStarted())),
      Some(Event(79815450, 18930,  DateTime.parse("2014-01-24T00:43:29.716+01").getMillis,DreamingStopped()))
      )
    events.zip(correct).foreach{ case (read, expected) => read shouldBe expected }
  }
}

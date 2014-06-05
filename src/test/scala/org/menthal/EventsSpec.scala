package org.menthal

import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import scala.io.Source
import org.joda.time.DateTime
import spray.json._
import DefaultJsonProtocol._

/**
 * Created by mark on 04.06.14.
 */
class EventsSpec extends FlatSpec with Matchers with BeforeAndAfterAll{
  val windowStateChange1 = WindowStateChanged("WhatsApp","com.whatsapp","[WhatsApp]")
  "getEventDataType" should "parse type 32 (WindowStateChange)" in {
    val edt = Event.getEventDataType("32", "\"[\\\\\"WhatsApp\\\\\",\\\\\"com.whatsapp/com.whatsapp.Conversation\\\\\",\\\\\"[WhatsApp]\\\\\"]\"")
    assert(edt.get == windowStateChange1)
  }
  it should "parse type 132 (WindowStateChangeBasic)" in {
    val edt = Event.getEventDataType("132", "\"[\\\\\"WhatsApp\\\\\",\\\\\"com.whatsapp/com.whatsapp.Conversation\\\\\",\\\\\"[WhatsApp]\\\\\"]\"")
    assert(edt.get == windowStateChange1)
  }
  it should "parse type 1006 (ScreenOff)" in {
    val edt = Event.getEventDataType("1006", "[]")
    assert(edt.get == ScreenOff())
  }
  it should "parse type 1014 (ScreenUnlock)" in {
    val edt = Event.getEventDataType("1014", "[]")
    assert(edt.get == ScreenUnlock())
  }
  it should "parse type 1017 (DreamingStarted)" in {
    val edt = Event.getEventDataType("1017", "[]")
    assert(edt.get == DreamingStarted())
  }
  it should "parse type 1018 (DreamingStopped)" in {
    val edt = Event.getEventDataType("1018", "[]")
    assert(edt.get == DreamingStopped())
  }
  it should "fail on unknown numbers" in {
    val edt = Event.getEventDataType("4093888", "bla")
    assert(edt == None)
  }

  "tryToParseLine" should "parse WindowStateChangedEvents" in {
    val line = "79822117\t22812\t2014-01-22 22:44:04.719+01\t32\t\"[\\\\\"WhatsApp\\\\\",\\\\\"com.whatsapp/com.whatsapp.Conversation\\\\\",\\\\\"[WhatsApp]\\\\\"]\""
    val result = Event.tryToParseLine(line)
    val expected = Event(79822117, 22812, windowStateChange1, DateTime.parse("2014-01-22T22:44:04.719+01"))
    assert(expected ==result.get)
  }
  "Creating events " should "work." in {
    val eventLines = Source.fromURL(getClass.getResource("/raw_events")).getLines()
    val events = eventLines.map(Event.tryToParseLine).toList
    val correct = List(
      None,
      Some(Event(79822117, 22812, WindowStateChanged("WhatsApp", "com.whatsapp", "[WhatsApp]"), DateTime.parse("2014-01-22T22:44:04.719+01"))),
      Some(Event(79822206, 23858, WindowStateChanged("WhatsApp", "com.whatsapp", "ComponentInfo{com.whatsapp/com.whatsapp.ContactInfo}"), DateTime.parse("2014-01-24T00:53:14.207+01"))),
      Some(Event(79822106, 18261,	ScreenOff(), DateTime.parse("2014-01-23T18:05:55.668+01"))),
      Some(Event(79822185, 23858, ScreenUnlock(), DateTime.parse("2014-01-24T00:51:06.82+01"))),
      None,
      Some(Event(79815448, 18930, DreamingStarted(), DateTime.parse("2014-01-24T00:43:31.724+01"))),
      Some(Event(79815450, 18930, DreamingStopped(), DateTime.parse("2014-01-24T00:43:29.716+01")))
    )
    events.zip(correct).foreach{ case (a, b) => assert(a==b)}
//    events.foreach(e => info(e+"\n"))
//    info("---------------------------")
//    correct.foreach(e => info(e+"\n"))
//    assert(correct==events)
  }
}

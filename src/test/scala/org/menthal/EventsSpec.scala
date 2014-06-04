package org.menthal

import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import scala.io.Source
import org.joda.time.DateTime

/**
 * Created by mark on 04.06.14.
 */
class EventsSpec extends FlatSpec with Matchers with BeforeAndAfterAll{
  "Creating events " should "work." in {
    val eventLines = Source.fromURL(getClass.getResource("/raw_events")).getLines()
    val events = eventLines.map(Event.tryToParseLine).toList
    val correct = List(
      None,
      Some(Event(79822117, 22812, WindowStateChanged("WhatsApp", "com.whatsapp", "[WhatsApp]"), DateTime.parse("2014-01-23T21:58:44.752+01"))),
      Some(Event(79822206, 23858, WindowStateChanged("WhatsApp", "com.whatsapp", "[WhatsApp]"), DateTime.parse("2014-01-24T00:53:14.207+01"))),
      Some(Event(79822106, 18261,	ScreenOff(), DateTime.parse("2014-01-23T18:05:55.668+01"))),
      Some(Event(79822185, 23858, ScreenUnlock(), DateTime.parse("2014-01-24T00:51:06.82+01"))),
      None,
      Some(Event(79815448, 23858, DreamingStarted(), DateTime.parse("2014-01-24T00:51:06.82+01"))),
      Some(Event(79815450, 18930, DreamingStopped(), DateTime.parse("2014-01-24T00:43:29.716+01")))
    )
//    events.foreach(e => info(e+"\n"))
//    info("---------------------------")
//    correct.foreach(e => info(e+"\n"))
    assert(correct==events)

  }
}

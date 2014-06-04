package org.menthal

import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import scala.io.Source

/**
 * Created by mark on 04.06.14.
 */
class EventsSpec extends FlatSpec with Matchers with BeforeAndAfterAll{
  "Creating events " should "work." in {
    val source = Source.fromURL(getClass.getResource("/raw_events"))
  }
}

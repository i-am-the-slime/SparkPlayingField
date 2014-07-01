package org.menthal.model.events

import org.joda.time.DateTime
import EventData._

case class Event(id:Long, userId:Long, time:Long, data:EventData) {

  override def toString:String = {
    val dataString = data.toString
    s"Event: id: $id, user: $userId, time: $time, data: $dataString)"
  }
}

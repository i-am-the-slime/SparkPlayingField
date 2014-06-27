package org.menthal.model.events

import org.joda.time.DateTime
import spray.json.DefaultJsonProtocol._
import spray.json._
import EventData._

import scala.util.Try

case class Event(id:Long, userId:Long, data:EventData, time:DateTime) {

  override def toString:String = {
    val dataString = data.toString
    s"Event: id: $id, user: $userId, time: $time, data: $dataString)"
  }
}

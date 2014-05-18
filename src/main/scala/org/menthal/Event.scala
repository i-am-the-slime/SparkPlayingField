package org.menthal

import org.joda.time.DateTime

/**
 * Created by mark on 18.05.14.
 */
sealed abstract class EventData(eventType:Long)

case class ScreenLock() extends EventData(Event.TYPE_SCREEN_LOCK)
case class ScreenUnlock() extends EventData(Event.TYPE_SCREEN_UNLOCK)
case class WindowStateChanged(appName:String, packageName:String, windowTitle:String)
  extends EventData(Event.TYPE_WINDOW_STATE_CHANGED)

case class Event(id:Long, userId:Long, data:EventData, time:DateTime){
  override def toString:String = {
    val dataString = data.toString
    s"Event: id: $id, user: $userId, time: $time, data: $dataString)"
  }
}
object Event{
  val TYPE_SCREEN_UNLOCK = 1005
  val TYPE_SCREEN_LOCK = 1006
  val TYPE_WINDOW_STATE_CHANGED = 32
}

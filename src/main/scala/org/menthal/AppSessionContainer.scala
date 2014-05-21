package org.menthal

import org.joda.time.DateTime

/**
 * Created by mark on 20.05.14.
 */
case class AppSessionContainer(xs:Vector[AppSession]) {
  def +(other:AppSessionContainer):AppSessionContainer = {
    (this.xs.lastOption, other.xs.headOption) match {
      //Empty lists
     case (None, None) => this
     case (Some(a:AppSession), None) => this
     case (None, Some(b:AppSession)) => other

     case (Some(a:Start), Some(b:Stop)) =>
       val session = Session(a.time, b.time, a.app.get)
       AppSessionContainer(this.xs.dropRight(1) ++ (session :: Nil)) + AppSessionContainer(other.xs.tail)

     case (Some(a:Lock), Some(b:Unlock)) =>
       val app = { if(b.app.isDefined) b.app else a.app }
       AppSessionContainer(this.xs.dropRight(1)) + AppSessionContainer(other.xs.updated(0, Start(b.time, app)))

     case (Some(a:Session), Some(b:Unlock)) =>
       AppSessionContainer(this.xs ++ other.xs.updated(0, Unlock(b.time, Some(a.app))))

     case (Some(a:Session), Some(b:Lock)) =>
       AppSessionContainer(this.xs ++ other.xs.updated(0, Lock(b.time, Some(a.app))))

     case (Some(a:Lock), Some(b:Session)) =>
       AppSessionContainer(this.xs.dropRight(1) ++ other.xs.updated(0, Lock(a.time, Some(b.app))))

     case (Some(a:Start), Some(b:Unlock)) =>
       AppSessionContainer(this.xs.dropRight(1) ++ other.xs.updated(0, Unlock(b.time, a.app)))

     case (Some(a:Lock), Some(b:Stop)) =>
       AppSessionContainer(this.xs) + AppSessionContainer(other.xs.drop(1))

     case (Some(a:Lock), Some(b:Start)) =>
       val i = this.xs.length-1
       AppSessionContainer(this.xs.updated(i,Lock(a.time, b.app))) + AppSessionContainer(other.xs.drop(1))

     //Base case: Just join the two
     case (Some(a:AppSession), Some(b:AppSession)) =>
       AppSessionContainer(this.xs ++ other.xs)
    }
  }
  override def toString:String = "\n"+xs.toString
}

sealed abstract class AppSession()
case class Session(start:DateTime, end:DateTime, app:String) extends AppSession()
case class Locked(start:DateTime, end:DateTime) extends AppSession()

case class Start(time:DateTime, app:Option[String]) extends AppSession()
case class Stop(time:DateTime) extends AppSession()
case class Lock(time:DateTime, app:Option[String]) extends AppSession()
case class Unlock(time:DateTime, app:Option[String]) extends AppSession()

object AppSessionContainer {
  def apply(sessions:AppSession*):AppSessionContainer = {
    AppSessionContainer(sessions.toVector)
  }
  def apply[A <: EventData](ev:Event[A]):AppSessionContainer = {
    val li =  ev.data match {
      case d:ScreenLock => Vector(Stop(ev.time), Lock(ev.time, None))
      case d:ScreenUnlock => Vector(Unlock(ev.time, None))
      case d:WindowStateChanged => Vector(Stop(ev.time), Start(ev.time, Some(d.packageName)))
    }
    AppSessionContainer(li)
  }
}

package org.menthal

import com.twitter.algebird.Monoid
import org.joda.time.DateTime
import org.menthal.model.events._
import scala.collection.immutable.Queue


/**
 * Created by mark on 20.05.14.
 * Update by Konrad 26.05.14
 */
object DateTimeImplicits {
  implicit def longToDateTime(timestamp:Long) = new DateTime(timestamp)
}
import DateTimeImplicits._

sealed abstract class AppSessionContainer {
  def sessions: Queue[AppSessionFragment]
  def toAppSessions(userId: Long): List[AppSession]
}

case class Empty() extends AppSessionContainer {
  def toAppSessions(userId: Long): List[AppSession] = List()
  val sessions = Queue.empty
}

case class Container(sessions: Queue[AppSessionFragment], last: AppSessionFragment) extends AppSessionContainer {
  def toQueue: Queue[AppSessionFragment] = sessions enqueue last

  def tail: AppSessionContainer = if (sessions.isEmpty) Empty() else Container(sessions.tail, last)

  def head: AppSessionFragment = sessions.headOption.getOrElse(last)

  def merge(other: AppSessionContainer): Container = {
    other match {
      case Container(otherSessions,otherLast) =>
        Container(this.toQueue ++ otherSessions, otherLast)
      case Empty() =>
        this
    }
  }

  def update(newFragment: AppSessionFragment): Container = {
    (last, newFragment) match {

      //Unlocks - they are usually special cases so we describe them first
      case (Unlock(t,app), _) => //add app session at the end and run update again with same argument
        Container(this.toQueue, Session(t, newFragment.time, app)) update newFragment
      case (_, Unlock(t, None)) => //update app kept in unlock
        Container(this.toQueue, Unlock(t, last.app))
      case (_, Unlock(_, Some(_))) => //we don't need to change anything
        Container(this.toQueue, newFragment)

      //Locks
      case (Lock(t,_), _) => //locks just eat up everything and changes its app until it hits unlock
        Container(sessions, Lock(t, newFragment.app))

      //Apps
      case (Session(t1, _, app1), Session(_, tEnd2, app2)) if app1 == app2 => //Merge same apps together
        Container(sessions, Session(t1, tEnd2, app1))
      case (Session(t1, _, app1), Lock(t2, None)) => //We extend session and set app kept in lock
        Container(sessions enqueue Session(t1, t2, app1), Lock(t2, app1))
      case (Session(t, _, app), _) => //We extend session
        Container(sessions enqueue Session(t, newFragment.time, app), newFragment)

      //Default case - if list covers only Unlocks, Lock, Start and Sessions it is unnecessary
      case _ => Container(this.toQueue, newFragment)
    }
  }
  override def toString:String = "\n" + sessions.toString + " " +  last.toString

  def toAppSessions(userId: Long): List[AppSession] = {
    this.toQueue.toList.flatMap(_.toAppSession(userId))
  }

}


sealed abstract class AppSessionFragment {
  val time: Long
  val app: Option[String]
  def toAppSession(userId: Long): Option[AppSession]
}


case class Session(time: Long, end: Long, app: Option[String]) extends AppSessionFragment
{
  override def toString = "\nSession\t" +
    time.toString("HH:mm:ss:ms\t") +
    app.getOrElse("") +
    "\t" + end.minus(time.getMillis+7200000).toString("HH:mm:ss\t") +
    end.toString("\n\t\tHH:mm:ss:ms\t")

  def toAppSession(userId: Long): Option[AppSession] = {
    app match {
      case None => None
      case appName => Some(new AppSession(userId, time, end - time, appName.get, appName.get))
    }
  }
}

case class Lock(time: Long, app: Option[String] = None) extends AppSessionFragment
{
  override def toString = "\nLock\t" + time.toString("HH:mm:ss:ms\t") + app.getOrElse("")
  def toAppSession(userId: Long): Option[AppSession] = None
}

case class Unlock(time: Long, app: Option[String] = None) extends AppSessionFragment
{
  override def toString = "\nUnlock\t" + time.toString("HH:mm:ss:ms\t") + app.getOrElse("")
  def toAppSession(userId: Long): Option[AppSession] = None
}

object AppSessionContainer {
  val handledEvents = Set[Class[_]](
    classOf[ScreenOff],
    classOf[WindowStateChanged],
    classOf[ScreenUnlock],
    classOf[DreamingStarted]
  )

  def eventToAppSessionFragment(ev: MenthalEvent): AppSessionFragment = {
    ev match {
      case so: CCScreenOff => Lock(so.time, None)
      case ds: CCDreamingStarted => Lock(ds.time, None)
      case su: CCScreenUnlock => Unlock(su.time, None)
      case d: CCWindowStateChanged => Session(d.time, d.time, Some(d.packageName))
    }
  }

  def apply(events: AppSessionFragment*): AppSessionContainer = {
    AppSessionContainer(Queue(events.dropRight(1) : _*), events.last)
  }

  def apply(sessions: Queue[AppSessionFragment], last: AppSessionFragment): AppSessionContainer = {
    Container(sessions, last)
  }

  def apply(ev: MenthalEvent): AppSessionContainer = {
    Container(Queue(), eventToAppSessionFragment(ev))
  }
}



object AppSessionMonoid extends Monoid[AppSessionContainer] {
  implicit val appSessionMonoid : Monoid[AppSessionContainer] = AppSessionMonoid
  override def zero = Empty()

  override def plus(left: AppSessionContainer, right: AppSessionContainer): AppSessionContainer = (left, right) match {
    case (Empty(), _) => right
    case (_, Empty()) => left

    case (l: Container, r: Container) =>
      val lPlusOne = l update r.head
      val rMinusOne = r.tail
      if (lPlusOne.last != r.head) //recursion is necessary because r changed
        this.plus(lPlusOne, rMinusOne)
      else //r stayed same so no recursion
        lPlusOne merge rMinusOne
  }
}


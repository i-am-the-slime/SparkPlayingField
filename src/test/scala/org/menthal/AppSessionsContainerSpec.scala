package org.menthal

import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
import org.joda.time.DateTime
import scala.util.Random

/**
 * Created by mark on 20.05.14.
 */
class AppSessionsContainerSpec extends FlatSpec with Matchers with BeforeAndAfterAll{

  val now = DateTime.now()
  val later = now plusMinutes 1

  val sessionA1 = Session(start=now, end=now.plusMinutes(5), app="com.beer")
  val sessionA2 = Session(start=now plusMinutes 10 , end=now plusMinutes 11 , app="com.meat")

  val sessionB1 = Session(start=now plusMinutes 15 , end=now plusMinutes 16 , app="com.beer")
  val sessionB2 = Session(start=now plusMinutes 20 , end=now plusMinutes 21 , app="com.beer")

  it should "convert ScreenLock to Stop(_), Lock(_)" in {
    AppSessionContainer(Event[ScreenLock](0, 0, ScreenLock(), now)) should be (
    AppSessionContainer(Stop(now), Lock(now, None)))
  }
  it should "convert ScreenUnlock to Unlock(_), Start(_)" in {
    AppSessionContainer(Event[ScreenUnlock](0, 0, ScreenUnlock(), now)) should be(
    AppSessionContainer(Unlock(now, None)))
  }
  it should "convert WindowStateChange to Stop(_), Start(A)" in {
    val wsc = WindowStateChanged("", "com.app", "")
    AppSessionContainer(Event[WindowStateChanged](0, 0, wsc, now)) should be(
    AppSessionContainer(Stop(now), Start(now, Some("com.app"))))
  }

//  it should "add lists of AppSessions" in {
//    val sessionsA = AppSessionContainer(sessionA1, sessionA2)
//    val sessionsB = AppSessionContainer(sessionB1, sessionB2)
//    val correct = AppSessionContainer(sessionA1, sessionA2, sessionB1, sessionB2)
//    sessionsA + sessionsB should be (correct)
//  }

  it should "add Start(A) and Stop(_)" in {
    val result = AppSessionContainer(Start(now, Some("app"))) +
      AppSessionContainer(Stop(later))
    val correct = AppSessionContainer(Session(now, later, "app"))
    result should be (correct)
  }

  it should "add Session(A) + Unlock(_)" in {
    val sessionOfA = Session(now, later, "A")
    val result = AppSessionContainer(sessionOfA) +
    AppSessionContainer(Unlock(later, None))
    val correct = AppSessionContainer(sessionOfA, Unlock(later, Some("A")))
    result should be (correct)
  }

  it should "add Session(A) + Lock(_)" in {
    val sessionOfA = Session(now, later, "A")
    val result = AppSessionContainer(sessionOfA) +
    AppSessionContainer(Lock(later, None))
    val correct = AppSessionContainer(sessionOfA, Lock(later, Some("A")))
    result should be (correct)
  }

  it should "add Lock(A) + Session(B)" in {
    val lockOfA =  Lock(now, Some("A"))
    val sessionOfB = Session(later, later plusMinutes 1, "B")
    val result = AppSessionContainer(lockOfA) + AppSessionContainer(sessionOfB)
    val correct = AppSessionContainer(Lock(now, Some("B")))
    result should be (correct)
  }

  it should "add Lock(A) + Unlock(_)" in {
    val lockOfA = Lock(now, Some("A"))
    val unlockOfNothing = Unlock(later, None)
    val result = AppSessionContainer(lockOfA) + AppSessionContainer(unlockOfNothing)
    val correct = AppSessionContainer(Start(later, Some("A")))
    result should be (correct)
  }

  it should "update Unlock and discard Start in Start(A) + Unlock(_)" in {
    val start = Start(now, Some("A"))
    val unlockOfA = Unlock(later, None)
    val result = AppSessionContainer(start) + AppSessionContainer(unlockOfA)
    val correct = AppSessionContainer(Unlock(later, Some("A")))
    result should be (correct)
  }

  it should "discard Stop in Lock() + Stop(_)" in {
    val lockOfNone = Lock(now, None)
    val stop = Stop(later)
    val result = AppSessionContainer(lockOfNone) + AppSessionContainer(stop)
    val correct = AppSessionContainer(Lock(now, None))
    result should be (correct)
  }

  it should "update the start app for Lock() + Start() and discard the start" in {
    val lockOfNone = Lock(now, None)
    val start = Start(later, Some("Pig"))
    val result = AppSessionContainer(lockOfNone) + AppSessionContainer(start)
    val correct = AppSessionContainer(Lock(now, Some("Pig")))
    result should be (correct)
  }

  "A long example" should "work" in {
    val now = DateTime.now
    val events = Vector(
      Event[WindowStateChanged](0,0,WindowStateChanged("", "A", ""), now),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "B", ""), now plusMinutes 1),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "C", ""), now plusMinutes 2),
      Event[ScreenLock](0,0, ScreenLock(), now plusMinutes 3),
//      Event[WindowStateChanged](0,0,WindowStateChanged("", "D", ""), now plusMinutes 2),
      Event[ScreenUnlock](0,0, ScreenUnlock(), now plusMinutes 4),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "E", ""), now plusMinutes 5)
    ) map (event => AppSessionContainer(event)) reduce (_+_)
    val correct = AppSessionContainer(
      Stop(now),
      Session(now, now plusMinutes 1, "A"),
      Session(now plusMinutes 1, now plusMinutes 2, "B"),
      Session(now plusMinutes 2, now plusMinutes 3, "C"),
      Session(now plusMinutes 4, now plusMinutes 5, "C"),
      Start(now plusMinutes 5, Some("E"))
    )
//    events.xs.foreach(x => info(x.toString))
    events should be (correct)
  }

  "A long example starting somewhere else" should "still work" in {
    val now = DateTime.now
    val events = Vector(
      Event[WindowStateChanged](0,0,WindowStateChanged("", "B", ""), now plusMinutes 1),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "C", ""), now plusMinutes 2),
      Event[ScreenLock](0,0, ScreenLock(), now plusMinutes 3),
      //      Event[WindowStateChanged](0,0,WindowStateChanged("", "D", ""), now plusMinutes 2),
      Event[ScreenUnlock](0,0, ScreenUnlock(), now plusMinutes 4),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "E", ""), now plusMinutes 5)
    ) map (event => AppSessionContainer(event)) reduce (_+_)
    val correct = AppSessionContainer(
      Stop(now plusMinutes 1),
      Session(now plusMinutes 1, now plusMinutes 2, "B"),
      Session(now plusMinutes 2, now plusMinutes 3, "C"),
      Session(now plusMinutes 4, now plusMinutes 5, "C"),
      Start(now plusMinutes 5, Some("E"))
    )
    events should be (correct)
  }

  "WindowStateChanges during locked state" should "still reduce fine" in {
    val now = DateTime.now
    val events = Vector(
      Event[WindowStateChanged](0,0,WindowStateChanged("", "A", ""), now),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "B", ""), now plusMinutes 1),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "C", ""), now plusMinutes 2),
      Event[ScreenLock](0,0, ScreenLock(), now plusMinutes 3),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "D", ""), now plusMinutes 2),
      Event[ScreenUnlock](0,0, ScreenUnlock(), now plusMinutes 4),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "E", ""), now plusMinutes 5)
    ) map (event => AppSessionContainer(event)) reduce (_+_)
    val correct = AppSessionContainer(
      Stop(now),
      Session(now, now plusMinutes 1, "A"),
      Session(now plusMinutes 1, now plusMinutes 2, "B"),
      Session(now plusMinutes 2, now plusMinutes 3, "C"),
      Session(now plusMinutes 4, now plusMinutes 5, "D"),
      Start(now plusMinutes 5, Some("E"))
    )
    events should be (correct)
  }

  val rand = new Random()
  var time = DateTime.now()
  def generateEvent():Event[_ <: EventData] = {
    val eventData= rand.nextInt() % 5 match {
      case 0 => ScreenLock()
      case 1 => ScreenUnlock()
      case _ => WindowStateChanged("","App Name","")
    }
    time = time plusMinutes rand.nextInt()%5+1
    Event[eventData.type](1,1,eventData, time)
  }

  "A super long example with random events" should "not break" in {
    val basic = (1 to 50).map(x => generateEvent())
    basic.foreach(x=>info(x.toString))
    info("--")
    val events = basic.map(x => AppSessionContainer(x)) reduce (_+_)
    events.xs.foreach(x => info(x.toString))
  }
}

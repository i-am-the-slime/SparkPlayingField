package org.menthal

import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
import org.joda.time.DateTime
import scala.util.Random
import com.twitter.algebird.Operators._
import org.menthal.AppSessionMonoid.appSessionMonoid
import scala.collection.immutable.Queue

/**
 * Created by mark on 20.05.14.
 */
class AppSessionsContainerSpec extends FlatSpec with Matchers with BeforeAndAfterAll{

  val now = DateTime.now()
  val later = now plusMinutes 1

  val sessionA1 = Session(time=now, end=now.plusMinutes(5), app=Some("com.beer"))
  val sessionA2 = Session(time=now plusMinutes 10 , end=now plusMinutes 11 , app=Some("com.meat"))

  val sessionB1 = Session(time=now plusMinutes 15 , end=now plusMinutes 16 , app=Some("com.beer"))
  val sessionB2 = Session(time=now plusMinutes 20 , end=now plusMinutes 21 , app=Some("com.beer"))

  "Calling AppSessionContainer(...)" should "convert ScreenLock to Stop(_), Lock(_)" in {
    AppSessionContainer(Event[ScreenOff](0, 0, ScreenOff(), now)) should be (
    AppSessionContainer(Lock(now,None)))
  }
  it should "convert ScreenUnlock to Unlock(_), Start(_)" in {
    AppSessionContainer(Event[ScreenUnlock](0, 0, ScreenUnlock(), now)) should be(
    AppSessionContainer(Unlock(now, None)))
  }
  it should "convert WindowStateChange to Stop(_), Start(A)" in {
    val wsc = WindowStateChanged("", "com.app", "")
    AppSessionContainer(Event[WindowStateChanged](0, 0, wsc, now)) should be(
    AppSessionContainer(Start(now, Some("com.app"))))
  }

  val container = Container(Queue(sessionA1, sessionA2), sessionB1)
  val containerWithEmptyQueue = Container(Queue(), sessionA1)

  "The Container data structure" should
    "have a toQueue function that returns the internal queue with the last element appended" in {
     assert(container.toQueue == Queue(sessionA1, sessionA2, sessionB1))
     assert(containerWithEmptyQueue.toQueue == Queue(sessionA1))
  }

  it should "have a head function that returns the first element in the queue or the last variable instead" in {
    assert(container.head == sessionA1)
    assert(containerWithEmptyQueue.head == sessionA1)
  }

  it should "have a tail function that returns itself without the first element" in {
    assert(container.tail == Container(Queue(sessionA2), sessionB1))
    assert(containerWithEmptyQueue.tail == Empty())
  }

  it should "have a merge function which creates a new AppSession from this and another one" in {
    val result1 = Container(Queue(sessionA1, sessionA2, sessionB1), sessionA1)
    val result2 = Container(Queue(sessionA1, sessionA2), sessionB1)
    assert(container.merge(containerWithEmptyQueue) == result1)
    assert(container.merge(Empty()) == result2)
  }

  "Arithmetic on AppSession containers" should "add Session(A) + Unlock(_)" in {
    val sessionOfA = Session(now, later, Some("A"))
    val result = AppSessionContainer(sessionOfA) +
    AppSessionContainer(Unlock(later, None))
    val correct = AppSessionContainer(sessionOfA, Unlock(later, Some("A")))
    result should be (correct)
  }

  it should "add Session(A) + Lock(_)" in {
    val sessionOfA = Session(now, later, Some("A"))
    val result = AppSessionContainer(sessionOfA) +
    AppSessionContainer(Lock(later, None))
    val correct = AppSessionContainer(sessionOfA, Lock(later, Some("A")))
    result should be (correct)
  }

  it should "add Lock(A) + Session(B)" in {
    val lockOfA =  Lock(now, Some("A"))
    val sessionOfB = Session(later, later plusMinutes 1, Some("B"))
    val result = AppSessionContainer(lockOfA) + AppSessionContainer(sessionOfB)
    val correct = AppSessionContainer(Lock(now, Some("B")))
    result should be (correct)
  }

  it should "add Lock(A) + Unlock(_)" in {
    val lockOfA = Lock(now, Some("A"))
    val unlockOfNothing = Unlock(later, None)
    val result = AppSessionContainer(lockOfA) + AppSessionContainer(unlockOfNothing)
    val correct = AppSessionContainer(lockOfA, Unlock(later, Some("A")))
    //val correct = AppSessionContainer(Start(later, Some("A")))
    result should be (correct)
  }

  it should "update Unlock and discard Start in Start(A) + Unlock(_)" in {
    val start = Start(now, Some("A"))
    val unlockOfA = Unlock(later, None)
    val result = AppSessionContainer(start) + AppSessionContainer(unlockOfA)
    //val correct = AppSessionContainer(Unlock(later, Some("A")))
    val correct = AppSessionContainer(start, Unlock(later, Some("A")))
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
      Event[ScreenOff](0,0, ScreenOff(), now plusMinutes 3),
//      Event[WindowStateChanged](0,0,WindowStateChanged("", "D", ""), now plusMinutes 2),
      Event[ScreenUnlock](0,0, ScreenUnlock(), now plusMinutes 4),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "E", ""), now plusMinutes 5)
    ) map (event => AppSessionContainer(event)) reduce (_+_)
    val correct = AppSessionContainer(
      //Stop(now),
      Session(now, now plusMinutes 1, Some("A")),
      Session(now plusMinutes 1, now plusMinutes 2, Some("B")),
      Session(now plusMinutes 2, now plusMinutes 3, Some("C")),
      Lock(now plusMinutes 3, Some("C")), //added
      Unlock(now plusMinutes 4, Some("C")), //added
      Session(now plusMinutes 4, now plusMinutes 5, Some("C")),
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
      Event[ScreenOff](0,0, ScreenOff(), now plusMinutes 3),
      //      Event[WindowStateChanged](0,0,WindowStateChanged("", "D", ""), now plusMinutes 2),
      Event[ScreenUnlock](0,0, ScreenUnlock(), now plusMinutes 4),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "E", ""), now plusMinutes 5)
    ) map (event => AppSessionContainer(event)) reduce (_+_)
    val correct = AppSessionContainer(
      //Stop(now plusMinutes 1),
      Session(now plusMinutes 1, now plusMinutes 2, Some("B")),
      Session(now plusMinutes 2, now plusMinutes 3, Some("C")),
      Lock(now plusMinutes 3, Some("C")), //added
      Unlock(now plusMinutes 4, Some("C")), //added
      Session(now plusMinutes 4, now plusMinutes 5, Some("C")),
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
      Event[ScreenOff](0,0, ScreenOff(), now plusMinutes 3),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "D", ""), now plusMinutes 2),
      Event[ScreenUnlock](0,0, ScreenUnlock(), now plusMinutes 4),
      Event[WindowStateChanged](0,0,WindowStateChanged("", "E", ""), now plusMinutes 5)
    ) map (event => AppSessionContainer(event)) reduce (_+_)
    val correct = AppSessionContainer(
      Session(now, now plusMinutes 1, Some("A")),
      Session(now plusMinutes 1, now plusMinutes 2, Some("B")),
      Session(now plusMinutes 2, now plusMinutes 3, Some("C")),
      Lock(now plusMinutes 3, Some("D")), //added
      Unlock(now plusMinutes 4, Some("D")), //added
      Session(now plusMinutes 4, now plusMinutes 5, Some("D")),
      Start(now plusMinutes 5, Some("E"))
    )
    events should be (correct)
  }

  val apps = List("ShitApp", "FuckApp", "WhoreFace", "KabelJau")
  val rand = new Random()
  var time = DateTime.now()
  def generateEvent():Event[_ <: EventData] = {
    val eventData= rand.nextInt() % 5 match {
      case 0 => ScreenOff()
      case 1 => ScreenUnlock()
      case _ => WindowStateChanged("", apps(Math.abs(rand.nextInt()) % 4) ,"")
    }
    time = time plusMinutes rand.nextInt()%5+1
    Event[eventData.type](1,1,eventData, time)
  }

  "A long example with random events" should "not break" in {
    val basic = (1 to 30).map(x => generateEvent())
    basic.foreach(x=>
      info(x.time.toString("HH:MM:SS  ") + x.data.toString)
    )
    info("--")
    val events = basic.map(x => AppSessionContainer(x)) reduce (_+_)
    events.sessions.foreach(x =>
      info(x.time.toString("HH:MM:SS  ") + x.toString)
    )
  }
}
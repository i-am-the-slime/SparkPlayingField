package org.menthal

import org.menthal.aggregations.tools._
import org.menthal.model.Granularity
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
import org.joda.time.DateTime
import scala.util.Random
import com.twitter.algebird.Operators._
import scala.collection.immutable.Queue
import org.menthal.model.events._
import org.menthal.model.implicits.DateImplicits._
import org.menthal.aggregations.tools.AppSessionMonoid._

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
    AppSessionContainer(CCScreenOff(0, 0, now)) should be (
    AppSessionContainer(Lock(now,None)))
  }

  it should "convert ScreenUnlock to Unlock(_), Start(_)" in {
    AppSessionContainer(CCScreenUnlock(0, 0, now )) should be(
    AppSessionContainer(Unlock(now, None)))
  }

  it should "convert ScreenLock to Unlock(_), Start(_)" in {
    AppSessionContainer(CCScreenUnlock(0, 0, now)) should be(
      AppSessionContainer(Unlock(now, None)))
  }

  it should "convert WindowStateChange to Session(A)" in {
    AppSessionContainer(CCWindowStateChanged(0, 0, now, "", "com.app", "")) should be(
    AppSessionContainer(Session(now, now, Some("com.app"))))
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

  it should "update Unlock in Session(A) + Unlock(_)" in {
    val start = Session(now, now, Some("A"))
    val unlockOfA = Unlock(later, None)
    val result = AppSessionContainer(start) + AppSessionContainer(unlockOfA)
    //val correct = AppSessionContainer(Unlock(later, Some("A")))
    val correct = AppSessionContainer(start, Unlock(later, Some("A")))
    result should be (correct)
  }

  it should "update the start app for Lock() + Session() and discard the start" in {
    val lockOfNone = Lock(now, None)
    val start = Session(later, later, Some("Pig"))
    val result = AppSessionContainer(lockOfNone) + AppSessionContainer(start)
    val correct = AppSessionContainer(Lock(now, Some("Pig")))
    result should be (correct)
  }

  "A long example" should "work" in {
    val now = DateTime.now
    val events = Vector(
      CCWindowStateChanged(0,0,  now, "", "A", ""),
      CCWindowStateChanged(0,0,  now plusMinutes 1, "", "B", ""),
      CCWindowStateChanged(0,0,  now plusMinutes 2, "", "C", ""),
      CCScreenOff(0,0,  now plusMinutes 3),
//      Event[WindowStateChanged](0,0,WindowStateChanged("", "D", ""), now plusMinutes 2),
      CCScreenUnlock(0,0,  now plusMinutes 4),
      CCWindowStateChanged(0,0,  now plusMinutes 5, "", "E", "")
    ) map (event => AppSessionContainer(event)) reduce (_+_)
    val correct = AppSessionContainer(
      //Stop(now),
      Session(now, now plusMinutes 1, Some("A")),
      Session(now plusMinutes 1, now plusMinutes 2, Some("B")),
      Session(now plusMinutes 2, now plusMinutes 3, Some("C")),
      Lock(now plusMinutes 3, Some("C")), //added
      Unlock(now plusMinutes 4, Some("C")), //added
      Session(now plusMinutes 4, now plusMinutes 5, Some("C")),
      Session(now plusMinutes 5, now plusMinutes 5, Some("E"))
    )
//    events.xs.foreach(x => info(x.toString))
    events should be (correct)
  }

  "A long example starting somewhere else" should "still work" in {
    val now = DateTime.now
    val events = Vector(
      CCWindowStateChanged(0,0,  now plusMinutes 1, "", "B", ""),
      CCWindowStateChanged(0,0,  now plusMinutes 2, "", "C", ""),
      CCScreenOff(0,0,  now plusMinutes 3),
      //      Event[WindowStateChanged](0,0,WindowStateChanged("", "D", ""), now plusMinutes 2),
      CCScreenUnlock(0,0,  now plusMinutes 4),
      CCWindowStateChanged(0,0,  now plusMinutes 5, "", "E", "")
    ) map (event => AppSessionContainer(event)) reduce (_+_)
    val correct = AppSessionContainer(
      //Stop(now plusMinutes 1),
      Session(now plusMinutes 1, now plusMinutes 2, Some("B")),
      Session(now plusMinutes 2, now plusMinutes 3, Some("C")),
      Lock(now plusMinutes 3, Some("C")), //added
      Unlock(now plusMinutes 4, Some("C")), //added
      Session(now plusMinutes 4, now plusMinutes 5, Some("C")),
      Session(now plusMinutes 5, now plusMinutes 5, Some("E"))
    )
    events should be (correct)
  }

  "WindowStateChanges during locked state" should "still reduce fine" in {
    val now = DateTime.now
    val events = Vector(
      CCWindowStateChanged(0,0, now.getMillis, "", "A", ""),
      CCWindowStateChanged(0,0, (now plusMinutes 1).getMillis, "", "B", ""),
      CCWindowStateChanged(0,0, (now plusMinutes 2).getMillis, "", "C", ""),
      CCScreenOff(0,0, (now plusMinutes 3).getMillis),
      CCWindowStateChanged(0,0, (now plusMinutes 4).getMillis, "", "D", ""),
      CCScreenUnlock(0,0, (now plusMinutes 5).getMillis),
      CCWindowStateChanged(0,0, (now plusMinutes 6).getMillis, "", "E", "")
    ) map (event => AppSessionContainer(event)) reduce(_ + _)
    val correct = AppSessionContainer(
      Session(now, now plusMinutes 1, Some("A")),
      Session(now plusMinutes 1, now plusMinutes 2, Some("B")),
      Session(now plusMinutes 2, now plusMinutes 3, Some("C")),
      Lock(now plusMinutes 3, Some("D")), //added
      Unlock(now plusMinutes 5, Some("D")), //added
      Session(now plusMinutes 5, now plusMinutes 6, Some("D")),
      Session(now plusMinutes 6, now plusMinutes 6, Some("E"))
    )
    events should be (correct)
  }

  val apps = List("ShitApp", "FuckApp", "WhoreFace", "KabelJau")
  val rand = new Random()
  var time = DateTime.now()
  time = time plusMinutes rand.nextInt()%5+1
  def generateEvent():MenthalEvent = {
    rand.nextInt() % 5 match {
      case 0 => CCScreenOff(1,1, time.getMillis)
      case 1 => CCScreenUnlock(1,1, time.getMillis)
      case _ => CCWindowStateChanged(1,1, time.getMillis, "", apps(Math.abs(rand.nextInt()) % 4) ,"")
    }
  }

  "A long example with random events" should "not break" in {
    val basic = (1 to 30).map(x => generateEvent())
//    basic.foreach(x=>
//      info(new DateTime(x.time).toString("HH:MM:SS  ") + x.data.toString)
//    )
//    info("--")
    val events = basic.map(x => AppSessionContainer(x)) reduce (_+_)
//    events.sessions.foreach(x =>
//      info(new DateTime(x.time).toString("HH:MM:SS  ") + x.toString)
//    )
  }

  "App Container to AppSession test" should "convert 'em correctly" in {

    val container = AppSessionContainer(
      Session(now, now plusMinutes 1, None),
      Session(now plusMinutes 1, now plusMinutes 2, Some("B")),
      Session(now plusMinutes 2, now plusMinutes 3, Some("C")),
      Lock(now plusMinutes 3, Some("D")), //added
      Unlock(now plusMinutes 5, Some("D")), //added
      Session(now plusMinutes 5, now plusMinutes 6, Some("D")),
      Session(now plusMinutes 6, now plusMinutes 6, Some("E"))
    )
    val sessions = container.toAppSessions(1)
    val correctSessions = List(
      new AppSession(1L, (now plusMinutes 1).getMillis, 60000L, "B"),
      new AppSession(1L, (now plusMinutes 2).getMillis, 60000L, "C"),
      new AppSession(1L, (now plusMinutes 5).getMillis, 60000L, "D"),
      new AppSession(1L, (now plusMinutes 6).getMillis, 0L, "E"))
    sessions should be (correctSessions)
  }
}

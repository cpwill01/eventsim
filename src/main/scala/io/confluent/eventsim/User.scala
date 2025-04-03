package io.confluent.eventsim

import java.io.Serializable
import java.time.LocalDateTime

import io.confluent.eventsim.config.ConfigFromFile

class User(val alpha: Double,
           val beta: Double,
           val startTime: LocalDateTime,
           val initialSessionStates: scala.collection.mutable.Map[(String, String), WeightedRandomThingGenerator[State]],
           val auth: String,
           val props: Map[String, Any],
           var device: Map[String, Any],
           val initialLevel: String
          ) extends Serializable with Ordered[User] {

  val userId = Counters.nextUserId
  var session = new Session(
    Some(Session.pickFirstTimeStamp(startTime, alpha, beta)),
    alpha, beta, initialSessionStates, auth, initialLevel)
  var isChurned = false
  var isFirstNotGuestEvent = session.currentState.auth != "Guest"
  var isGuest = !isFirstNotGuestEvent

  override def compare(that: User) =
    (that.session.nextEventTimeStamp, this.session.nextEventTimeStamp) match {
      case (None, None) => 0
      case (_: Some[LocalDateTime], None) => -1
      case (None, _: Some[LocalDateTime]) => 1
      case (thatValue: Some[LocalDateTime], thisValue: Some[LocalDateTime]) =>
        thatValue.get.compareTo(thisValue.get)
    }

  def nextEvent(): Unit = nextEvent(0.0)

  def nextEvent(prAttrition: Double) = {
    session.incrementEvent()
    if (session.done) {
      if (TimeUtilities.rng.nextDouble() < prAttrition ||
        session.currentState.auth == ConfigFromFile.churnedState.getOrElse("")) {
        session.nextEventTimeStamp = None
        isChurned = true
      }
    } else if ((session.currentState.auth != "Guest") && isGuest) {
      isGuest = false
      isFirstNotGuestEvent = true
    } else if (isFirstNotGuestEvent) {
      isFirstNotGuestEvent = false
    }
  }

  def isSessionDone(): Boolean = session.done

  def nextSession() = {
    assert(session.done)
    if (session.nextEventTimeStamp.isDefined) {
      // Generate next session if user is not churned
      session = session.nextSession
    }
  }

  def tsToString(ts: LocalDateTime) = ts.toString

  def nextEventTimeStampString =
    tsToString(this.session.nextEventTimeStamp.get)

}


package io.confluent.eventsim.events.SessionStartEnd

import io.confluent.eventsim.avro.SessionStartEnd

import scala.collection.JavaConversions._

class AvroConstructor() extends io.confluent.eventsim.events.AvroConstructor[SessionStartEnd] with Constructor {

  def schema = SessionStartEnd.getClassSchema

  var eventBuilder = SessionStartEnd.newBuilder()

  def setTs(n: Long) = eventBuilder.setTs(n)

  def setUserId(n: Long) = eventBuilder.setUserId(n)

  def setSessionId(n: Long) = eventBuilder.setSessionId(n)

  def setLevel(s: String) = eventBuilder.setLevel(s)

  def setItemInSession(i: Int) = eventBuilder.setItemInSession(i)

  def setUserDetails(m: Map[String, Any]): Unit =
    eventBuilder.setUserDetails(m.asInstanceOf[Map[CharSequence, AnyRef]])

  def setDeviceDetails(m: Map[String, Any]): Unit =
    eventBuilder.setDeviceDetails(m.asInstanceOf[Map[CharSequence, AnyRef]])

  def setTag(s: String) = eventBuilder.setTag(s)

  def setAuth(s: String) = eventBuilder.setAuth(s)
  
  def setIsEnd(boolean: Boolean) = eventBuilder.setIsEnd(boolean)

  def start() = {
    eventBuilder = SessionStartEnd.newBuilder(eventBuilder)
  }

  def end() = eventBuilder.build()


}

package io.confluent.eventsim.events.SessionStartEnd

trait Constructor extends io.confluent.eventsim.events.Constructor {
  def setAuth(s: String)
  
  def setIsEnd(boolean: Boolean)
}

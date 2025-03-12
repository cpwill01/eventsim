package io.confluent.eventsim.events.SessionStartEnd

class JSONConstructor() extends io.confluent.eventsim.events.JSONConstructor with Constructor {
  def setAuth(s: String) = generator.writeStringField("auth", s)
  
  def setIsEnd(boolean: Boolean) = generator.writeBooleanField("isEnd", boolean)
}

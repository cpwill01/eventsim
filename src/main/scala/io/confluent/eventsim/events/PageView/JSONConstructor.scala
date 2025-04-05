package io.confluent.eventsim.events.PageView

class JSONConstructor() extends io.confluent.eventsim.events.JSONConstructor with Constructor {
  def setPage(s: String) = generator.writeStringField("page", s)

  def setAuth(s: String) = generator.writeStringField("auth", s)

  def setMethod(s: String) = generator.writeStringField("method", s)

  def setStatus(i: Int) = generator.writeNumberField("status", i)

  def setAdRevenue(f: Float) = generator.writeNumberField("ad_revenue", f)
}

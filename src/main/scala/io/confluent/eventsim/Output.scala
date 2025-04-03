package io.confluent.eventsim

import java.io.{File, FileOutputStream}
import java.time.ZoneOffset
import java.util.Properties

import io.confluent.eventsim.config.ConfigFromFile
import io.confluent.eventsim.events.Auth.Constructor
import io.confluent.eventsim.events.StatusChange.{AvroConstructor, JSONConstructor}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by jadler on 1/13/16.
  */
object Output {

  // place to put all the output related code

  trait canwrite {
    def write()

    def flushAndClose()
  }

  private class FileEventWriter(val constructor: events.Constructor, val file: File) extends Object with canwrite {
    val out = new FileOutputStream(file, true)

    def write() = out.write(constructor.end().asInstanceOf[Array[Byte]])

    override def flushAndClose(): Unit = {
      out.flush(); out.close()
    }
  }

  private class KafkaEventWriter(val constructor: events.Constructor, val topic: String, val brokers: String) extends Object with canwrite {

    val props = new Properties()
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    //if (constructor.isInstanceOf[JSONConstructor]) "org.apache.kafka.common.serialization.ByteArraySerializer"
    //else "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)

    val producer = new KafkaProducer[Object, Object](props)
    System.err.println("Created kafka writer: " + topic + " : " + brokers)
    def write() = {
      val value =
        constructor match {
          case constructor1: events.AvroConstructor[Object] => constructor1.serialize(constructor.end())
          case _ => constructor.end()
        }
      val pr = new ProducerRecord[Object, Object](topic, value)
      producer.send(pr)
    }

    override def flushAndClose(): Unit = {
      producer.flush(); producer.close();
    }
  }

  val authConstructor: Constructor =
    if (Main.useAvro) new io.confluent.eventsim.events.Auth.AvroConstructor()
    else new io.confluent.eventsim.events.Auth.JSONConstructor()

  val listenConstructor: io.confluent.eventsim.events.Listen.Constructor =
    if (Main.useAvro) new io.confluent.eventsim.events.Listen.AvroConstructor()
    else new io.confluent.eventsim.events.Listen.JSONConstructor()

  val pageViewConstructor: io.confluent.eventsim.events.PageView.Constructor =
    if (Main.useAvro) new io.confluent.eventsim.events.PageView.AvroConstructor()
    else new io.confluent.eventsim.events.PageView.JSONConstructor()

  val statusChangeConstructor: io.confluent.eventsim.events.StatusChange.Constructor =
    if (Main.useAvro) new AvroConstructor()
    else new JSONConstructor()

  val sessionStartEndConstructor: io.confluent.eventsim.events.SessionStartEnd.Constructor =
    if (Main.useAvro) new io.confluent.eventsim.events.SessionStartEnd.AvroConstructor()
    else new io.confluent.eventsim.events.SessionStartEnd.JSONConstructor()

  val userInfoConstructor: io.confluent.eventsim.events.Constructor =
    if (Main.useAvro) new AvroConstructor()
    else new JSONConstructor()

  val kbl = Main.ConfFromOptions.kafkaBrokerList
  val dirName = new File(if (Main.ConfFromOptions.outputDir.isSupplied) Main.ConfFromOptions.outputDir.get.get else "output")

  if (!dirName.exists())
    dirName.mkdir()

  var authEventWriter =
    if (kbl.isSupplied) new KafkaEventWriter(authConstructor, "auth_events", kbl.get.get)
    else new FileEventWriter(authConstructor, new File(dirName, "auth_events.json"))
  var listenEventWriter =
    if (kbl.isSupplied) new KafkaEventWriter(listenConstructor, "listen_events", kbl.get.get)
    else new FileEventWriter(listenConstructor, new File(dirName, "listen_events.json"))
  var pageViewEventWriter =
    if (kbl.isSupplied) new KafkaEventWriter(pageViewConstructor, "page_view_events", kbl.get.get)
    else new FileEventWriter(pageViewConstructor, new File(dirName, "page_view_events.json"))
  var statusChangeEventWriter =
    if (kbl.isSupplied) new KafkaEventWriter(statusChangeConstructor, "status_change_events", kbl.get.get)
    else new FileEventWriter(statusChangeConstructor, new File(dirName, "status_change_events.json"))
  var sessionStartEndEventWriter =
    if (kbl.isSupplied) new KafkaEventWriter(sessionStartEndConstructor, "session_start_end_events", kbl.get.get)
    else new FileEventWriter(sessionStartEndConstructor, new File(dirName, "session_start_end_events.json"))
  var userInfoWriter =
    if (kbl.isSupplied) new KafkaEventWriter(userInfoConstructor, "user_info", kbl.get.get)
    else new FileEventWriter(userInfoConstructor, new File(dirName, "user_info.json"))

  def flushAndClose(): Unit = {
    authEventWriter.flushAndClose()
    listenEventWriter.flushAndClose()
    pageViewEventWriter.flushAndClose()
    statusChangeEventWriter.flushAndClose()
    sessionStartEndEventWriter.flushAndClose()
    userInfoWriter.flushAndClose()
  }

  def writeUserInfo(session: Session, userId: Int, props: Map[String, Any]): Unit = {
    if (!Main.disableUserInfoOutput) {
      userInfoConstructor.start
      userInfoConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
      userInfoConstructor.setUserId(userId)
      userInfoConstructor.setUserDetails(props)
      userInfoConstructor.setLevel(session.currentState.level)
      if (Main.tag.isDefined) {
        userInfoConstructor.setTag(Main.tag.get)
      }
      userInfoWriter.write
    }
  }

  def writeEvents(session: Session, device: Map[String, Any], userId: Int, props: Map[String, Any]): Unit = {

    val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
    if ((session.done || !session.previousState.isDefined) && !Main.disableSessionStartEndOutput) {
      sessionStartEndConstructor.start
      sessionStartEndConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
      sessionStartEndConstructor.setSessionId(session.sessionId)
      sessionStartEndConstructor.setLevel(session.currentState.level)
      sessionStartEndConstructor.setItemInSession(session.itemInSession)
      sessionStartEndConstructor.setAuth(session.currentState.auth)
      sessionStartEndConstructor.setIsEnd(session.done)
      sessionStartEndConstructor.setDeviceDetails(device)  
      if (showUserDetails) {
        sessionStartEndConstructor.setUserId(userId)
        sessionStartEndConstructor.setUserDetails(props)
        if (Main.tag.isDefined)
          sessionStartEndConstructor.setTag(Main.tag.get)
      }
      sessionStartEndEventWriter.write

      if (session.done) {
        return
      }

    }

    if (!Main.disablePageViewOutput) {
      pageViewConstructor.start
      pageViewConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
      pageViewConstructor.setSessionId(session.sessionId)
      pageViewConstructor.setPage(session.currentState.page)
      pageViewConstructor.setAuth(session.currentState.auth)
      pageViewConstructor.setMethod(session.currentState.method)
      pageViewConstructor.setStatus(session.currentState.status)
      pageViewConstructor.setLevel(session.currentState.level)
      pageViewConstructor.setItemInSession(session.itemInSession)
      pageViewConstructor.setDeviceDetails(device)
      if (showUserDetails) {
        pageViewConstructor.setUserId(userId)
        pageViewConstructor.setUserDetails(props)
        if (Main.tag.isDefined)
          pageViewConstructor.setTag(Main.tag.get)
      }
    }


    if (session.currentState.page == "NextSong") {
      if (!Main.disablePageViewOutput) {
        pageViewConstructor.setArtist(session.currentSong.get._2)
        pageViewConstructor.setTitle(session.currentSong.get._3)
        pageViewConstructor.setDuration(session.currentSong.get._4)
      }

      if (!Main.disableListenOutput) {
        listenConstructor.start()
        listenConstructor.setArtist(session.currentSong.get._2)
        listenConstructor.setTitle(session.currentSong.get._3)
        listenConstructor.setDuration(session.currentSong.get._4)
        listenConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
        listenConstructor.setSessionId(session.sessionId)
        listenConstructor.setAuth(session.currentState.auth)
        listenConstructor.setLevel(session.currentState.level)
        listenConstructor.setItemInSession(session.itemInSession)
        listenConstructor.setDeviceDetails(device)
        if (showUserDetails) {
          listenConstructor.setUserId(userId)
          listenConstructor.setUserDetails(props)
          if (Main.tag.isDefined)
            listenConstructor.setTag(Main.tag.get)
        }
        listenEventWriter.write
      }
    }

    if ((session.currentState.page == "Submit Downgrade" || session.currentState.page == "Submit Upgrade") && !Main.disableStatusChangeOutput) {
      statusChangeConstructor.start()
      statusChangeConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
      statusChangeConstructor.setSessionId(session.sessionId)
      statusChangeConstructor.setAuth(session.currentState.auth)
      statusChangeConstructor.setLevel(session.currentState.level)
      statusChangeConstructor.setItemInSession(session.itemInSession)
      statusChangeConstructor.setDeviceDetails(device)
      if (showUserDetails) {
        statusChangeConstructor.setUserId(userId)
        statusChangeConstructor.setUserDetails(props)
        if (Main.tag.isDefined)
          statusChangeConstructor.setTag(Main.tag.get)
      }
      statusChangeEventWriter.write
    }

    if (session.previousState.isDefined && session.previousState.get.page == "Login" && !Main.disableAuthOutput) {
      authConstructor.start()
      authConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
      authConstructor.setSessionId(session.sessionId)
      authConstructor.setLevel(session.currentState.level)
      authConstructor.setItemInSession(session.itemInSession)
      authConstructor.setDeviceDetails(device)
      if (showUserDetails) {
        authConstructor.setUserId(userId)
        authConstructor.setUserDetails(props)
        if (Main.tag.isDefined)
          authConstructor.setTag(Main.tag.get)
      }
      authConstructor.setSuccess(session.currentState.auth == "Logged In")
      authEventWriter.write
    }

    if (!Main.disablePageViewOutput) {
      pageViewEventWriter.write
    }
  }

  def setFileSuffix(fileSuffix: String): Unit = {
    if (!Main.disableAuthOutput) authEventWriter = new FileEventWriter(authConstructor, new File(dirName, "auth_events" + fileSuffix + ".json"))
    if (!Main.disableListenOutput) listenEventWriter = new FileEventWriter(listenConstructor, new File(dirName, "listen_events" + fileSuffix + ".json"))
    if (!Main.disablePageViewOutput) pageViewEventWriter = new FileEventWriter(pageViewConstructor, new File(dirName, "page_view_events" + fileSuffix + ".json"))
    if (!Main.disableStatusChangeOutput) statusChangeEventWriter = new FileEventWriter(statusChangeConstructor, new File(dirName, "status_change_events" + fileSuffix + ".json"))
    if (!Main.disableSessionStartEndOutput) sessionStartEndEventWriter = new FileEventWriter(sessionStartEndConstructor, new File(dirName, "session_start_end_events" + fileSuffix + ".json"))
    if (!Main.disableUserInfoOutput) userInfoWriter = new FileEventWriter(userInfoConstructor, new File(dirName, "user_info" + fileSuffix + ".json"))
  }
}

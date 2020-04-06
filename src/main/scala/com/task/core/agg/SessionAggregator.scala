package com.task.core.agg


import java.io.Serializable
import java.util.UUID

import com.task.core.models._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}


import scala.collection.SortedSet

object SessionAggregator extends Aggregator[Event, SessionsWithRawEvents, List[EventOverSession]] with Serializable {

  override def zero: SessionsWithRawEvents = SessionsWithRawEvents(SortedSet(), List())

  override def reduce(sessionsWithRaw: SessionsWithRawEvents, ev: Event): SessionsWithRawEvents = {
    ev.eventType match {
      case "app_open" =>
        val newSession = Session(
          List(ev), UUID.randomUUID().toString,
          ev.eventTime,
          ev.attributes.flatMap(_.get("campaign_id")),
          ev.attributes.flatMap(_.get("channel_id"))
        )
        sessionsWithRaw.copy(sessions = sessionsWithRaw.sessions ++ SortedSet(newSession))
      case _ =>
        sessionsWithRaw.copy(rawEvents = sessionsWithRaw.rawEvents :+ ev)
    }
  }

  private def insertOneEvent(raw: SessionsWithRawEvents, event: Event): SessionsWithRawEvents = {
    val (after, before) = raw.sessions
      .partition(session => session.startTime.after(event.eventTime))
    before.lastOption match {
      case Some(session) =>
        val updatedBefore = before - session
        val updatedSession = session.addEvent(event)
        SessionsWithRawEvents(updatedBefore + updatedSession ++ after, List.empty)
      case _ => raw
    }
  }


  private def insertAllEvents(raw: SessionsWithRawEvents): SessionsWithRawEvents = {
    @scala.annotation.tailrec
    def inner(raw: SessionsWithRawEvents, events: List[Event]): SessionsWithRawEvents =
      events match {
        case Nil => raw
        case head :: tail =>
          val updatedSessions = insertOneEvent(raw, head)
          inner(updatedSessions, tail)
      }
    inner(raw, raw.rawEvents)
  }

  override def merge(b1: SessionsWithRawEvents, b2: SessionsWithRawEvents): SessionsWithRawEvents = b1 ::: b2

  override def finish(sessionsWithRaw: SessionsWithRawEvents): List[EventOverSession] =
    for {
      sessions <- insertAllEvents(sessionsWithRaw).sessions.toList
      ev <- sessions.events
    } yield EventOverSession(
      ev.eventType,
      ev.eventTime,
      ev.userId,
      sessions.campaignId,
      sessions.channelIid,
      sessions.sessionId,
      ev.attributes
    )

  override def bufferEncoder: Encoder[SessionsWithRawEvents] = Encoders.kryo(classOf[SessionsWithRawEvents])

  override def outputEncoder: Encoder[List[EventOverSession]] = Encoders.product[List[EventOverSession]]
}
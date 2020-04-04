package com.task.core.agg


import java.io.Serializable
import java.util.UUID

import com.task.core.models._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}


import scala.collection.SortedSet

class SessionAggregator extends Aggregator[Event, SessionsWithRawEvents, List[EventOverSession]] with Serializable {

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

  private def insertOneEvent(sessions: SortedSet[Session], event: Event): SortedSet[Session] = {
    val (after, before): (SortedSet[Session], SortedSet[Session]) = sessions
      .partition(session => session.startTime.after(event.eventTime))
    val updated: SortedSet[Session] = before.lastOption match {
      case Some(session) =>
        val updatedBefore = before - session
        val updatedSession = session.copy(
          events = session.events :+ event
        )
        updatedBefore + updatedSession
    }
    updated ++ after
  }

  @scala.annotation.tailrec
  private def insertAllEvents(sessions: SortedSet[Session], events: List[Event]): SortedSet[Session] = {
    events match {
      case Nil => sessions
      case head :: tail =>
        val updatedSessions = insertOneEvent(sessions, head)
        insertAllEvents(updatedSessions, tail)
    }
  }

  override def merge(b1: SessionsWithRawEvents, b2: SessionsWithRawEvents): SessionsWithRawEvents =
    SessionsWithRawEvents(insertAllEvents(b1.sessions, b1.rawEvents) ++ insertAllEvents(b2.sessions, b2.rawEvents), List())

  override def finish(sessionsWithRaw: SessionsWithRawEvents): List[EventOverSession] =
    for {
      sess <- sessionsWithRaw.sessions.toList
      ev <- sess.events
    } yield EventOverSession(
      ev.eventType,
      ev.eventTime,
      ev.userId,
      sess.campaignId,
      sess.channelIid,
      sess.sessionId,
      ev.attributes
    )

  override def bufferEncoder: Encoder[SessionsWithRawEvents] = Encoders.kryo(classOf[SessionsWithRawEvents])

  override def outputEncoder: Encoder[List[EventOverSession]] = Encoders.product[List[EventOverSession]]
}
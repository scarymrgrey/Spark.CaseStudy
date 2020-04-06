package com.task.core.agg


import java.io.Serializable
import java.util.UUID

import com.task.core.models._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

class SessionAggregator extends Aggregator[Event, SessionsWithRawEvents, List[EventOverSession]] with Serializable {

  override def zero: SessionsWithRawEvents = SessionsWithRawEvents(List(), List())

  override def reduce(raw: SessionsWithRawEvents, ev: Event): SessionsWithRawEvents = {
    ev.eventType match {
      case "app_open" =>
        val newSession = Session(
          List(ev), UUID.randomUUID().toString,
          ev.eventTime,
          ev.attributes.flatMap(_.get("campaign_id")),
          ev.attributes.flatMap(_.get("channel_id"))
        )
        SessionsWithRawEvents(raw.sessions :+ newSession, raw.rawEvents)
      case _ =>
        SessionsWithRawEvents(raw.sessions, raw.rawEvents :+ ev)
    }
  }

  private def insertOneEvent(raw: SessionsWithRawEvents, event: Event): SessionsWithRawEvents = {
    val (after, before) = raw.sessions
      .partition(session => session.startTime.after(event.eventTime))
    before.sorted match {
      case head :+ session =>
        val updated = head :+ session.copy(
          events = session.events :+ event
        )
        SessionsWithRawEvents(updated ::: after, List())
      case _ => raw
    }
  }

  private def insertAllEvents(agg: SessionsWithRawEvents): SessionsWithRawEvents = {

    @scala.annotation.tailrec
    def inner(sessWithRaw: SessionsWithRawEvents, restEvents: List[Event]): SessionsWithRawEvents = {
      restEvents match {
        case head :: tail =>
          val updatedSessions = insertOneEvent(sessWithRaw, head)
          inner(updatedSessions, tail)
        case Nil =>
          sessWithRaw
      }
    }
    inner(agg, agg.rawEvents)
  }

  override def merge(b1: SessionsWithRawEvents, b2: SessionsWithRawEvents): SessionsWithRawEvents = b1 ::: b2

  override def finish(sessionsWithRaw: SessionsWithRawEvents): List[EventOverSession] =
    for {
      sess <- insertAllEvents(sessionsWithRaw).sessions
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

  override def bufferEncoder: Encoder[SessionsWithRawEvents] = Encoders.product[SessionsWithRawEvents]

  override def outputEncoder: Encoder[List[EventOverSession]] = Encoders.product[List[EventOverSession]]
}
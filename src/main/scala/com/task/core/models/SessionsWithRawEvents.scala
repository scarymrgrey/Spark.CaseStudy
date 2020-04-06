package com.task.core.models

import java.sql.Timestamp

import scala.collection.immutable.List

case class SessionsWithRawEvents(sessions: List[Session], rawEvents: List[Event]) {
  def :::(prefix: SessionsWithRawEvents): SessionsWithRawEvents = SessionsWithRawEvents(sessions ::: prefix.sessions, rawEvents ::: prefix.rawEvents)
}

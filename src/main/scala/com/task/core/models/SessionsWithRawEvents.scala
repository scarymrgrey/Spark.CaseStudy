package com.task.core.models

import scala.collection.SortedSet

case class SessionsWithRawEvents(sessions: SortedSet[Session], rawEvents: List[Event])

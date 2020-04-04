package com.task.core.models

import java.sql.Timestamp

case class Event(userId: String, eventId: String, eventType: String,eventTime: Timestamp, attributes: Option[Map[String, String]])

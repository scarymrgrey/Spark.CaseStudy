package com.task.core.models

import java.sql.Timestamp

case class EventOverSession(eventType: String, eventTime: Timestamp, userId: String, campaignId: Option[String], channelIid: Option[String], sessionId: String, attributes: Option[Map[String, String]])

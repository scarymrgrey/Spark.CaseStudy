package com.task.core.models

import java.sql.Timestamp

case class Session(events: List[Event], sessionId: String, startTime: Timestamp, campaignId: Option[String], channelIid: Option[String]) extends Ordered[Session] {
  override def compare(that: Session): Int = startTime.compareTo(that.startTime)

  def addEvent(ev: Event): Session = Session(events :+ ev, sessionId, startTime, campaignId, channelIid)
}

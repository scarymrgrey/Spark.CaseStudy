package com.task.core.agg

import java.io.Serializable
import java.util.UUID
import com.task.core.models._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import scala.collection.SortedSet

object SessionAggregator extends Aggregator[Event, SortedSet[Event], List[EventOverSession]] with Serializable {

  override def reduce(list: SortedSet[Event], ev: Event): SortedSet[Event] = list + ev

  override def merge(b1: SortedSet[Event], b2: SortedSet[Event]): SortedSet[Event] = b1 ++ b2

  override def bufferEncoder: Encoder[SortedSet[Event]] = Encoders.kryo[SortedSet[Event]]

  override def outputEncoder: Encoder[List[EventOverSession]] = Encoders.product[List[EventOverSession]]

  override def zero: SortedSet[Event] = SortedSet.empty[Event]

  override def finish(events: SortedSet[Event]): List[EventOverSession] =
    events.foldLeft(List.empty[EventOverSession])({ (list, ev) =>
      val newEv = ev.eventType match {
        case "app_open" =>
          EventOverSession(
            "app_open",
            ev.eventTime,
            ev.userId,
            ev.attributes.flatMap(_.get("campaign_id")),
            ev.attributes.flatMap(_.get("channel_id")),
            UUID.randomUUID().toString,
            ev.attributes
          )
        case _ =>
          list match {
            case _ :+ tailEl =>
              EventOverSession(
                ev.eventType,
                ev.eventTime,
                ev.userId,
                tailEl.campaignId,
                tailEl.channelIid,
                tailEl.sessionId,
                ev.attributes
              )
          }
      }
      list :+ newEv
    })
}
package com.task.core.jobs

import com.task.core.agg.SessionAggregator
import com.task.core.models.Event
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.task.transformations.SessionTransformations
import org.apache.spark.sql.functions._

class MarketingAnalysisJobProcessor(rawEvents: DataFrame, rawPurchases: DataFrame)(implicit spark: SparkSession) {

  type SessionsAndPurchases = (DataFrame, DataFrame)
  import spark.implicits._

  def getPurchasesWithSessions: SessionsAndPurchases = {
    val sessions = rawEvents
      .transform(SessionTransformations.enrichWithSession)
    val purchases = sessions
      .transform(SessionTransformations.transformWithJoin(rawPurchases))

    (sessions, purchases)
  }

  def purchasesViaAggregator: DataFrame = {
    rawEvents
      .as[Event]
      .groupByKey(r => r.userId)
      .agg(SessionAggregator.toColumn)
      .flatMap(_._2)
      .toDF()
      .transform(SessionTransformations.transformWithJoin(rawPurchases))

  }

  def topCompaigns(top: Int, purchases: DataFrame)(implicit spark: SparkSession): DataFrame = {
    purchases
      .where('isConfirmed === true)
      .groupBy('campaignId)
      .agg(sum('billingCost).as("revenue"))
      .orderBy('revenue.desc)
      .select('campaignId)
      .limit(top)
  }

  def channelsEngagementPerformance(sessions: DataFrame)(implicit spark: SparkSession): DataFrame = {
    sessions
      .groupBy('campaignId, 'channelIid)
      .agg(countDistinct('sessionId) as "uniqueSessions")
      .orderBy('uniqueSessions.desc)
      .select('channelIid)
      .limit(1)
  }
}

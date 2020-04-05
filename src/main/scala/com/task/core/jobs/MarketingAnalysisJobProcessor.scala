package com.task.core.jobs

import com.task.core.agg.SessionAggregator
import com.task.core.models.Event
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.task.transformations.SessionTransformations
import org.apache.spark.sql.expressions.Window
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

  def topCompaigns(top: Int, purchases: DataFrame): DataFrame = {
    purchases
      .where('isConfirmed === true)
      .groupBy('campaignId)
      .agg(sum('billingCost).as("revenue"))
      .orderBy('revenue.desc)
      .select('campaignId)
      .limit(top)
  }

  def channelsEngagementPerformance(sessions: DataFrame): DataFrame = {
    sessions
      .groupBy('campaignId, 'channelIid)
      .agg(first('channelIid).over(Window.partitionBy('campaignId).orderBy(countDistinct('sessionId).desc)) as "TopChannel")
      .drop('channelIid)
      .distinct()
  }
}

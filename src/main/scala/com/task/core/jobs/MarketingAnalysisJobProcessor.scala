package com.task.core.jobs

import com.task.core.agg.SessionAggregator
import com.task.core.models.Event
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.task.transformations.SessionTransformations
import org.apache.spark.sql.functions._

class MarketingAnalysisJobProcessor(rawEvents: DataFrame, rawPurchases: DataFrame) {

  type SessionsAndPurchases = (DataFrame, DataFrame)

  //TASK 1.1
  def getPurchasesWithSessions(implicit spark: SparkSession): SessionsAndPurchases = {
    val sessions = rawEvents
      .transform(SessionTransformations.enrichWithSession)
    val purchases = sessions
      .transform(SessionTransformations.transformWithJoin(rawPurchases))

    (sessions, purchases)
  }

  def showPurchasesViaAggregator(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    rawEvents
      .as[Event]
      .groupByKey(r => r.userId)
      .agg(new SessionAggregator().toColumn)
      .flatMap(_._2)
      .toDF()
      .transform(SessionTransformations.transformWithJoin(rawPurchases))
      .show()
  }

  def showTopCampaigns(purchases: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    purchases
      .where('isConfirmed === true)
      .groupBy('campaignId)
      .agg(sum('billingCost).as("revenue"))
      .orderBy('revenue.desc)
      .limit(10)
      .show()
  }

  def showChannelsEngagementPerformance(sessions: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    sessions
      .groupBy('campaignId,'channelIid)
      .agg(countDistinct('sessionId) as "uniqueSessions")
      .orderBy('uniqueSessions.desc)
      .select('channelIid)
      .limit(1)
      .show()
  }

}

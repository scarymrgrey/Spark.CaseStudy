package com.task.core.jobs

import com.task.core.agg.SessionAggregator
import com.task.core.models.Event
import com.task.transformations.EventsTransformations
import org.apache.spark.sql.{DataFrame, SparkSession}

class MarketingAnalysisJobProcessor(events: DataFrame, purchases: DataFrame) {

  val sessionTableName = "sessionsTemporary"
  val purchasesTableName = "aggregatedPurchasesTemporary"

  //TASK 1.1
  def saveAndGetPurchases(implicit spark: SparkSession): DataFrame = {
    events
      .transform(EventsTransformations.enrichWithSession)
      .createOrReplaceTempView(sessionTableName)

    spark.table(sessionTableName)
      .transform(EventsTransformations.transformWithJoin(purchases))
      .createOrReplaceTempView(purchasesTableName)

    spark
      .sql(s"select * from $purchasesTableName")
  }

  def purchasesViaAggregator(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    events
      .as[Event]
      .groupByKey(r => r.userId)
      .agg(new SessionAggregator().toColumn)
      .flatMap(_._2)
      .toDF()
      .transform(EventsTransformations.transformWithJoin(purchases))
  }

  def topCampaigns(implicit spark: SparkSession): DataFrame = {
    spark
      .sql(
        s"""select
           | campaignId, sum(billingCost) as revenue
           | from  $purchasesTableName
           | where isConfirmed = true
           | group by campaignId
           | order by revenue desc
           | limit 10""".stripMargin)

  }

  def channelsEngagementPerformance(implicit spark: SparkSession): DataFrame = {
    spark.sql(
      s"""select
         | channelIid
         | from $sessionTableName
         | group by campaignId, channelIid
         | order by count(distinct sessionId) desc
         | limit 1""".stripMargin)
  }

}

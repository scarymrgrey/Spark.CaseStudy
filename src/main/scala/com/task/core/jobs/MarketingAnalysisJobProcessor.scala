package com.task.core.jobs

import com.task.core.agg.SessionAggregator
import com.task.core.models.Event
import com.task.transformations.EventsTransformations
import org.apache.spark.sql.{DataFrame, SparkSession}

class MarketingAnalysisJobProcessor(events: DataFrame, purchases: DataFrame) {

  private val sessionTableName = "sessionsTemporary"
  private val purchasesTableName = "aggregatedPurchasesTemporary"

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
      .agg(SessionAggregator.toColumn)
      .flatMap(_._2)
      .transform(EventsTransformations.transformWithJoin(purchases))
  }

  def topCampaigns(top: Int)(implicit spark: SparkSession): DataFrame = {
    spark
      .sql(
        s"""select
           | campaignId as MarketingCampaign, sum(billingCost) as Revenue
           | from  $purchasesTableName
           | where isConfirmed = true
           | group by campaignId
           | order by revenue desc
           | limit $top""".stripMargin)

  }

  def channelsEngagementPerformance(implicit spark: SparkSession): DataFrame = {

//         select
//         | distinct campaignId as Campaign,
//         | first(channelIid) over(partition by campaignId order by count(distinct sessionId) desc) as TopChannel
//         | from $sessionTableName
//         | group by campaignId, channelIid

    // This is a second option to implement task using subquery instead of first() and distinct
    spark.sql(
      s"""
         |select campaignId as Campaign, channelIid as TopChannel
         |from
         |  (select campaignId,
         |  channelIid,
         |  row_number() over(partition by campaignId order by count(distinct sessionId) desc) as row
         |  from $sessionTableName
         |  group by campaignId, channelIid)
         |where row = 1
         """.stripMargin)
  }

}

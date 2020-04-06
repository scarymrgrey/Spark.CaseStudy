package com.task.core.jobs

import com.task.core.agg.SessionAggregator
import com.task.core.models.Event
import com.task.core.transformations.SessionTransformations
import org.apache.spark.sql.{DataFrame, SparkSession}

class MarketingAnalysisJobProcessor(events: DataFrame, purchases: DataFrame)(implicit spark: SparkSession) {
  val sessionTableName = "sessionTable"
  val purchasesTableName = "purchasesTable"
  import spark.implicits._
  def saveAndGetPurchases: DataFrame = {
    events
      .transform(SessionTransformations.enrichWithSession)
      .createOrReplaceTempView(sessionTableName)

    spark.table(sessionTableName)
      .transform(SessionTransformations.transformWithJoin(purchases))
      .createOrReplaceTempView(purchasesTableName)

    spark
      .sql(s"select * from $purchasesTableName")
  }

  def purchasesViaAggregator: DataFrame = {
    events
      .as[Event]
      .groupByKey(r => r.userId)
      .agg(SessionAggregator.toColumn)
      .flatMap(_._2)
      .toDF()
      .transform(SessionTransformations.transformWithJoin(purchases))
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
    spark.sql(
      s"""select
         | distinct campaignId as Campaign,
         | first(channelIid) over(partition by campaignId order by count(distinct sessionId) desc) as TopChannel
         | from $sessionTableName
         | group by campaignId, channelIid""".stripMargin)
  }

}

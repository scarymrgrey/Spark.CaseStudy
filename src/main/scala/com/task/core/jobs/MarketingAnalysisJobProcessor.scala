package com.task.core.jobs

import com.task.core.agg.SessionAggregator
import com.task.core.models.Event
import com.task.core.transformations.SessionTransformations
import org.apache.spark.sql.{DataFrame, SparkSession}

class MarketingAnalysisJobProcessor(events: DataFrame, purchases: DataFrame, sessionTableName: String, purchasesTableName: String) {

  def saveAndShowPurchases(implicit spark: SparkSession): Unit = {
    events
      .transform(SessionTransformations.enrichWithSession)
      .createOrReplaceTempView(sessionTableName)

    spark.table(sessionTableName)
      .transform(SessionTransformations.transformWithJoin(purchases))
      .createOrReplaceTempView(purchasesTableName)

    spark
      .sql(s"select * from $purchasesTableName")
      .show()
  }

  def showPurchasesViaAggregator(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    events
      .as[Event]
      .groupByKey(r => r.userId)
      .agg(SessionAggregator.toColumn)
      .flatMap(_._2)
      .toDF()
      .transform(SessionTransformations.transformWithJoin(purchases))
      .show()
  }

  def showTopCampaigns(implicit spark: SparkSession): Unit = {
    spark
      .sql(
        s"""select
           | campaignId, sum(billingCost) as revenue
           | from  $purchasesTableName
           | where isConfirmed = true
           | group by campaignId
           | order by revenue desc
           | limit 10""".stripMargin)
      .show()
  }

  def showChannelsEngagementPerformance(implicit spark: SparkSession): Unit = {    spark.sql(
    s"""select
       | distinct campaignId as Campaign,
       | first(channelIid) over(partition by campaignId order by count(distinct sessionId) desc) as TopChannel
       | from $sessionTableName
       | group by campaignId, channelIid""".stripMargin)
      .show()
  }

}

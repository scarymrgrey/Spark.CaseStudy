package com.task.core.jobs

import com.task.core.agg.SessionAggregator
import com.task.core.models.Event
import org.apache.spark.sql.{DataFrame, SparkSession}
import transformations.SessionTransformations

class MarketingAnalysisJobProcessor(events: DataFrame, purchases: DataFrame, sessionTableName: String, purchasesTableName: String) {

  //TASK 1.1
  def saveAndShowPurchases(implicit spark: SparkSession): Unit = {
    events
      .transform(SessionTransformations.enrichWithSession)
      .createOrReplaceTempView(sessionTableName)

    spark.table(sessionTableName)
      .transform(SessionTransformations.transformWithJoin(purchases))
      .createOrReplaceTempView(purchasesTableName)

    spark
      .sql(s"select * from $purchases")
      .show()
  }

  def showPurchasesViaAggregator(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    events
      .as[Event]
      .groupByKey(r => r.userId)
      .agg(new SessionAggregator().toColumn)
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

  def showChannelsEngagementPerformance(implicit spark: SparkSession): Unit = {
    spark.sql(
      s"""select
         | channelIid
         | from $sessionTableName
         | group by campaignId, channelIid
         | order by count(distinct sessionId) desc
         | limit 1""".stripMargin)
      .show()
  }

}

package com.task.transformations

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

object EventsTransformations {

  def enrichWithSession(events: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val windowLastOverUser = Window.partitionBy('userId).orderBy('eventTime).rowsBetween(Window.unboundedPreceding, 0)

    def lastInCol(col: Column) = last(col, ignoreNulls = true).over(windowLastOverUser)

    events
      .withColumn("session_start", when('eventType === "app_open", concat(lit("session_"), monotonically_increasing_id())))
      .withColumn("sessionId", lastInCol('session_start))
      .withColumn("campaignId", lastInCol($"attributes.campaign_id"))
      .withColumn("channelIid", lastInCol($"attributes.channel_id"))
  }

  def transformWithJoin[T](joinDF: DataFrame)(df: Dataset[T])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.join(joinDF, $"attributes.purchase_id" === 'purchaseId)
      .select($"purchases.*",
        'sessionId,
        'campaignId,
        'channelIid)
  }
}

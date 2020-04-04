package transformations

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{last, monotonically_increasing_id, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object SessionTransformations {

  def enrichWithSession(events: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val windowLastOverUser = Window.partitionBy('userId).orderBy('eventTime).rowsBetween(Window.unboundedPreceding, 0)
    def lastInCol(col: Column) = last(col, ignoreNulls = true).over(windowLastOverUser)
    events
      .orderBy('eventTime)
      .withColumn("session_start", when('eventType === "app_open", monotonically_increasing_id()))
      .withColumn("sessionId", lastInCol('session_start))
      .withColumn("campaignId", lastInCol($"attributes.campaign_id"))
      .withColumn("channelIid", lastInCol($"attributes.channel_id"))
  }

  def transformWithJoin(joinDF: DataFrame)(df: DataFrame)(implicit spark: SparkSession) : DataFrame ={
    import spark.implicits._
    df.join(joinDF, $"attributes.purchase_id" === 'purchaseId)
      .select($"purchases.*",
        'eventType,
        'sessionId,
        'campaignId,
        'channelIid)
  }
}

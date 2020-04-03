import java.io.Serializable
import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Encoder, Encoders, SparkSession}
import com.typesafe.config._
import pureconfig.ConfigSource

import collection.JavaConverters._

object MarketingAnalisysDriver {

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder
      .master("local[*]")
      .appName("spark test")
      .getOrCreate()

    import org.apache.spark.sql.types._
    import spark.implicits._
    val sessionTempTableName = "sessionsTemporary"
    val productsSchema = new StructType()
      .add("userId", StringType)
      .add("eventId", StringType)
      .add("eventTime", TimestampType)
      .add("eventType", StringType)
      .add("attributes", StringType, true)

    val events = spark
      .read
      .option("header", "true")
      .schema(productsSchema)
      .csv("mobile-app-clickstream_sample - mobile-app-clickstream_sample.csv")

    val purchases = spark
      .read
      .option("header", "true")
      .csv("purchases_sample - purchases_sample.csv")
      .as("purchases")

    val windowLastOverUser = Window.partitionBy('userId).orderBy('eventTime).rowsBetween(Window.unboundedPreceding, 0)

    def lastInCol(col: Column) = last(col, ignoreNulls = true).over(windowLastOverUser)

    //TASK 1.1
    events
      .withColumn("attributes", expr("substring(attributes,2,length(attributes)-2)"))
      .withColumn("attributes", from_json('attributes, MapType(StringType, StringType)))
      .orderBy('eventTime)
      .withColumn("session_start", when('eventType === "app_open", monotonically_increasing_id()))
      .withColumn("sessionId", lastInCol('session_start))
      .withColumn("campaignId", lastInCol($"attributes.campaign_id"))
      .withColumn("channelIid", lastInCol($"attributes.channel_id"))
      .createOrReplaceTempView(sessionTempTableName)

    val aggregatedPurchasesTableName = "aggregated_purchases"
    spark.table(sessionTempTableName)
      .join(purchases, $"attributes.purchase_id" === 'purchaseId)
      .select($"purchases.*",
        'eventType,
        'sessionId,
        'campaignId,
        'channelIid)
      .createOrReplaceTempView(aggregatedPurchasesTableName)

    spark
      .sql(s"select * from $aggregatedPurchasesTableName")
    //  .show()

    // TASK 2.1
    spark
      .sql(
        s"""select
           | campaignId, sum(billingCost) as revenue
           | from  $aggregatedPurchasesTableName
           | where isConfirmed = true
           | group by campaignId
           | order by revenue desc
           | limit 10""".stripMargin)
    // .show()

    //TASK 2.2
    spark.sql(s"""select
              | channelIid
              | from $sessionTempTableName
              | group by campaignId, channelIid
              | order by count(distinct sessionId) desc
              | limit 1""".stripMargin)
          .show()

    spark.stop()
  }
}
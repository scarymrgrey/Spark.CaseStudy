import java.io.Serializable
import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
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

    val productsSchema = new StructType()
      .add("userId", StringType)
      .add("eventId", StringType)
      .add("eventTime", TimestampType)
      .add("eventType", StringType)
      .add("attributes",StringType, true)

    val events = spark
      .read
      .option("header", "true")
      .schema(productsSchema)
      .csv("mobile-app-clickstream_sample - mobile-app-clickstream_sample.csv")

    val purchases = spark
      .read
      .option("header", "true")
      .csv("purchases_sample - purchases_sample.csv")

    events
      .withColumn("attributes", expr("substring(attributes,2,length(attributes)-2)"))
      .withColumn("attributes", from_json('attributes, MapType(StringType, StringType)))
      .orderBy('eventTime)
      .withColumn("session_start", when('eventType === "app_open", monotonically_increasing_id()))
      .join(purchases, $"attributes.purchase_id" === 'purchaseId, "left")
      .withColumn("campaignId",$"attributes.campaign_id")
      .withColumn("channelIid",$"attributes.channel_id")
      .show(truncate = false)
  }
}
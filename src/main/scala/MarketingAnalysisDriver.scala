import com.task.core.jobs.MarketingAnalysisJobProcessor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MarketingAnalysisDriver {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder
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
      .add("attributes", StringType, true)

    val rawEvents = spark
      .read
      .option("header", "true")
      .schema(productsSchema)
      .csv("mobile-app-clickstream_sample - mobile-app-clickstream_sample.csv")

    val events = rawEvents
      .withColumn("attributes", expr("substring(attributes,2,length(attributes)-2)"))
      .withColumn("attributes", from_json('attributes, MapType(StringType, StringType)))

    val purchases = spark
      .read
      .option("header", "true")
      .csv("purchases_sample - purchases_sample.csv")
      .as("purchases")

    val sessionsTn = "sessionsTemporary"
    val aggregatedPurchasesTn = "aggregatedPurchasesTemporary"
    val jobsProcessor = new MarketingAnalysisJobProcessor(events, purchases, sessionsTn, aggregatedPurchasesTn)
    import jobsProcessor._

    //TASK 1.1
    saveAndShowPurchases

    //TASK 1.2
    showPurchasesViaAggregator

    // TASK 2.1
    showTopCampaigns

    //TASK 2.2
    showChannelsEngagementPerformance

    spark.stop()
  }
}
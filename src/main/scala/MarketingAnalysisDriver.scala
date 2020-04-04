import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.task.infastructure.{AppSettings, WithConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import transformations.JsonTransformations

object MarketingAnalysisDriver extends WithConfig {

  type EventsAndPurchases = (DataFrame, DataFrame)

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSession()
    val settings = ConfigSource
      .fromConfig(conf)
      .at("analysis")
      .loadOrThrow[AppSettings]

    val (events, purchases) = loadFromSettings(settings, spark)

    val jobsProcessor = new MarketingAnalysisJobProcessor(events, purchases)
    import jobsProcessor._

    //TASK 1.1
    val (sessions, purchasesDataFrame) = getPurchases
    //TASK 1.2
    showPurchasesViaAggregator
    // TASK 2.1
    showTopCampaigns(purchasesDataFrame)
    //TASK 2.2
    showChannelsEngagementPerformance(sessions)

    spark.stop()
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder
      .master("local[*]")
      .appName("spark test")
      .getOrCreate()
  }

  def loadFromSettings(settings: AppSettings, spark: SparkSession): EventsAndPurchases = {

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
      .csv(settings.eventsFilePath)

    val events = rawEvents
        .transform(JsonTransformations.recover("attributes"))

    val purchases = spark
      .read
      .option("header", "true")
      .csv(settings.purchasesFilePath)
      .as("purchases")

    (events, purchases)
  }
}
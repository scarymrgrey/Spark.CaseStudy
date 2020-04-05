import com.task.core.jobs.MarketingAnalysisJobProcessor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try
import cats.effect.IO

object MarketingAnalysisDriver {

  def main(args: Array[String]): Unit = {

    val program = IO {
      SparkSession.builder
        .master("local[*]")
        .appName("spark test")
        .getOrCreate()

    }.bracket { spark =>
      IO(doJobs(spark))
    } { spark =>
      IO(spark.close())
    }

    program.unsafeRunSync()
  }

  def doJobs(implicit spark: SparkSession): Unit = {

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
      .csv("src/main/resources/mobile-app-clickstream_sample - mobile-app-clickstream_sample.csv")

    val events = rawEvents
      .withColumn("attributes", expr("substring(attributes,2,length(attributes)-2)"))
      .withColumn("attributes", from_json('attributes, MapType(StringType, StringType)))

    val purchases = spark
      .read
      .option("header", "true")
      .csv("src/main/resources/purchases_sample - purchases_sample.csv")
      .as("purchases")


    val jobsProcessor = new MarketingAnalysisJobProcessor(events, purchases)
    import jobsProcessor._

    //TASK 1.1
    saveAndGetPurchases
      .show()

    //TASK 1.2
    purchasesViaAggregator
      .show()

    // TASK 2.1
    topCampaigns(10)
      .show()

    //TASK 2.2
    channelsEngagementPerformance
      .show()
  }
}
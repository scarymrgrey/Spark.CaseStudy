import cats.effect.IO
import cats.syntax.functor._
import com.task.core.data.DataLoader
import com.task.core.jobs.MarketingAnalysisJobProcessor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MarketingAnalysisDriver extends DataLoader {

  def main(args: Array[String]): Unit = {

    val program = IO {
      SparkSession.builder
        .master("local[*]")
        .appName("spark test")
        .getOrCreate()

    }.bracket { spark =>
      IO(doJobs(spark))
    } { spark =>
      IO(spark.close()).void
    }

    program.unsafeRunSync()
  }

  def doJobs(implicit spark: SparkSession): Unit = {

    val (events, purchases) = loadData

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
import com.task.core.infrastructure.{DataLoad, Spark}
import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.task.core.transformations.JsonTransformations
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MarketingAnalysisDriver extends Spark with DataLoad {

  def main(args: Array[String]): Unit = {

    val (events, purchases) = loadData
    val sessionsTn = "sessionsTemporary"
    val aggregatedPurchasesTn = "aggregatedPurchasesTemporary"
    val jobsProcessor = new MarketingAnalysisJobProcessor(events, purchases, sessionsTn, aggregatedPurchasesTn)
    import jobsProcessor._

    //TASK 1.1
    saveAndShowPurchases
    //TASK 1.2
    showPurchasesViaAggregator
    //TASK 2.1
    showTopCampaigns
    //TASK 2.2
    showChannelsEngagementPerformance

    spark.stop()
  }
}
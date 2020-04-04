import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.task.infastructure.{DataLoader, Spark, WithSettings}

object MarketingAnalysisDriver extends Spark with WithSettings with DataLoader {

  def main(args: Array[String]): Unit = {

    val (events, purchases) = loadFromSettings

    val jobsProcessor = new MarketingAnalysisJobProcessor(events, purchases)
    import jobsProcessor._

    //TASK 1.1
    val (sessionsDataFrame, purchasesDataFrame) = getPurchases
    //TASK 1.2
    showPurchasesViaAggregator
    //TASK 2.1
    showTopCampaigns(purchasesDataFrame)
    //TASK 2.2
    showChannelsEngagementPerformance(sessionsDataFrame)

    spark.stop()
  }
}
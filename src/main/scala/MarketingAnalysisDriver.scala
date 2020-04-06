import com.task.core.infrastructure.{DataLoad, Spark}
import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.task.core.transformations.JsonTransformations
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MarketingAnalysisDriver extends Spark with DataLoad {

  def main(args: Array[String]): Unit = {

    val (events, purchases) = loadData
    val jobsProcessor = new MarketingAnalysisJobProcessor(events, purchases)
    import jobsProcessor._

    //TASK 1.1
    saveAndGetPurchases
      .show

    //TASK 1.2
    purchasesViaAggregator
      .show

    //TASK 2.1
    topCampaigns(10)
      .show
    
    //TASK 2.2
    channelsEngagementPerformance
      .show

    spark.stop()
  }
}
import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.task.infastructure.{DataLoader, Spark, WithSettings}
import org.apache.avro.generic.GenericData.StringType

object MarketingAnalysisDriver extends Spark with WithSettings with DataLoader {

  def main(args: Array[String]): Unit = {

    val (events, purchases) = loadFromSettings

    val jobsProcessor = new MarketingAnalysisJobProcessor(events, purchases)
    
    import jobsProcessor._
    import org.apache.spark.sql.functions.col

    //TASK 1.1
    val (sessionsDataFrame, purchasesDataFrame) = getPurchasesWithSessions
    sessionsDataFrame
      .cache()
      .withColumn("attributes", col("attributes").cast("string"))
      .repartition(1)
      .write
      .csv(settings.outputSessionsPath)

    //TASK 1.2
    showPurchasesViaAggregator
      .repartition(1)
      .write
      .csv(settings.outputPurchasesPath)

    //TASK 2.1
    showTopCampaigns(settings.topCompaniesToShow, purchasesDataFrame)
      .repartition(1)
      .write
      .csv(settings.outputTopCompaniesPath)

    //TASK 2.2
    showChannelsEngagementPerformance(sessionsDataFrame)
      .repartition(1)
      .write
      .csv(settings.outputChannelEngagementsPath)

    spark.stop()
  }
}
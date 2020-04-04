package com.task
import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.task.infastructure.{DataLoader, Spark, WithSettings}
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions.col

object MarketingAnalysisDriver extends Spark with WithSettings with DataLoader {

  def main(args: Array[String]): Unit = {

    val (events, purchases) = loadFromSettingsWithArgs(args)

    val jobsProcessor = new MarketingAnalysisJobProcessor(events, purchases)
    
    import jobsProcessor._

    //TASK 1.1
    val (sessionsDataFrame, purchasesDataFrame) = getPurchasesWithSessions
    sessionsDataFrame
      .cache()
      .withColumn("attributes", col("attributes").cast("string"))
      .repartition(1)
      .write
      .mode("overwrite")
      .csv(settings.outputSessionsPath)

    //TASK 1.2
    showPurchasesViaAggregator
      .repartition(1)
      .write
      .mode("overwrite")
      .csv(settings.outputPurchasesPath)

    //TASK 2.1
    showTopCampaigns(settings.topCompaniesToShow, purchasesDataFrame)
      .repartition(1)
      .write
      .mode("overwrite")
      .csv(settings.outputTopCompaniesPath)

    //TASK 2.2
    showChannelsEngagementPerformance(sessionsDataFrame)
      .repartition(1)
      .write
      .mode("overwrite")
      .csv(settings.outputChannelEngagementsPath)

    spark.stop()
  }
}
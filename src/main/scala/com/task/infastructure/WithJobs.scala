package com.task.infastructure

import com.task.MarketingAnalysisDriver.{loadFromSettingsWithArgs, settings}
import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import pureconfig.ConfigSource

trait WithJobs {

  def doJobs(events: DataFrame, purchases: DataFrame)(implicit spark: SparkSession) = {

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
    purchasesViaAggregator
      .repartition(1)
      .write
      .mode("overwrite")
      .csv(settings.outputPurchasesPath)

    //TASK 2.1
    topCompaigns(settings.topCompaniesToShow, purchasesDataFrame)
      .repartition(1)
      .write
      .mode("overwrite")
      .csv(settings.outputTopCompaniesPath)

    //TASK 2.2
    channelsEngagementPerformance(sessionsDataFrame)
      .repartition(1)
      .write
      .mode("overwrite")
      .csv(settings.outputChannelEngagementsPath)

  }
}

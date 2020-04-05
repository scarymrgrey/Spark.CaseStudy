package com.task.infastructure

import com.task.transformations.JsonTransformations
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataLoader {
  self: WithSettings =>
  type EventsAndPurchases = (DataFrame, DataFrame)

  def loadFromSettingsWithArgs(args: Array[String])(implicit spark : SparkSession): EventsAndPurchases = {

    val namedArgs = args.map(_.split("--")).map(y=>(y(0),y(1))).toMap
    val eventsInputPath = namedArgs.getOrElse("eventsInput", settings.eventsFilePath)
    val purchasesInputPath = namedArgs.getOrElse("purchasesInput", settings.purchasesFilePath)

    import org.apache.spark.sql.types._
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
      .csv(eventsInputPath)

    val events = rawEvents
      .transform(JsonTransformations.recover("attributes"))

    val purchases = spark
      .read
      .option("header", "true")
      .csv(purchasesInputPath)
      .as("purchases")

    (events, purchases)
  }
}

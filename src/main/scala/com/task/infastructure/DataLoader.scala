package com.task.infastructure

import com.task.transformations.JsonTransformations
import org.apache.spark.sql.DataFrame

trait DataLoader {
  self: Spark with WithSettings =>
  type EventsAndPurchases = (DataFrame, DataFrame)

  def loadFromSettings: EventsAndPurchases = {

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

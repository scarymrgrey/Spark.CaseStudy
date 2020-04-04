package com.task.core.infrastructure

import com.task.core.transformations.JsonTransformations
import org.apache.spark.sql.DataFrame

trait DataLoad { self: Spark =>
  type EventsWithPurchases = (DataFrame, DataFrame)

  def loadData: EventsWithPurchases = {
    import org.apache.spark.sql.types._
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
      .csv("mobile-app-clickstream_sample - mobile-app-clickstream_sample.csv")

    val events = rawEvents
      .transform(JsonTransformations.recover("attributes"))

    val purchases = spark
      .read
      .option("header", "true")
      .csv("purchases_sample - purchases_sample.csv")
      .as("purchases")

    (events,purchases)
  }
}

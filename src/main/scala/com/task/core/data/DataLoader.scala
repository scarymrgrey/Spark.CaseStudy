package com.task.core.data

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr, from_json}
import org.apache.spark.sql.types.{MapType, StringType, StructType, TimestampType}

trait DataLoader {
  type EventsWithPurchases = (DataFrame, DataFrame)

  def loadData()(implicit spark: SparkSession): EventsWithPurchases = {
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
      .csv("src/main/resources/mobile-app-clickstream_sample - mobile-app-clickstream_sample.csv")

    val events = rawEvents
      .withColumn("attributes", expr("substring(attributes,2,length(attributes)-2)"))
      .withColumn("attributes", from_json('attributes, MapType(StringType, StringType)))

    val purchases = spark
      .read
      .option("header", "true")
      .csv("src/main/resources/purchases_sample - purchases_sample.csv")
      .as("purchases")

    (events, purchases)
  }

}

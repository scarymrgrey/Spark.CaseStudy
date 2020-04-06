package com.task.core.infrastructure

import org.apache.spark.sql.SparkSession

trait Spark {
  implicit val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("spark test")
    .getOrCreate()
}
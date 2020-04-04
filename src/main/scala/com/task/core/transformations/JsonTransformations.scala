package com.task.core.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{expr, from_json}
import org.apache.spark.sql.types.{MapType, StringType}

object JsonTransformations {
  def recover(colName: String)(df: DataFrame): DataFrame = {
    df.withColumn(colName, expr(s"substring($colName,2,length($colName)-2)"))
      .withColumn(colName, from_json(col(colName), MapType(StringType, StringType)))
  }
}

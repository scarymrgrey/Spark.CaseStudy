package transformations

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{expr, from_json, last, monotonically_increasing_id, when}
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object JsonTransformations {
  type Transformation = DataFrame => DataFrame

  def recover(colName: String): Transformation =
    _.withColumn(colName, expr(s"substring($colName,2,length($colName)-2)"))
      .withColumn(colName, from_json(col(colName), MapType(StringType, StringType)))
}

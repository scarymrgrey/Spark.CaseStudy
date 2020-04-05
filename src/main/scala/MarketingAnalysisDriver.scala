package com.task

import MarketingAnalysisDriver.{doJobs, loadFromSettingsWithArgs}
import cats.effect.IO
import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.task.infastructure.{DataLoader, WithJobs, WithSettings}
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object MarketingAnalysisDriver extends WithSettings with DataLoader with WithJobs {

  def main(args: Array[String]): Unit = {

    val program = IO {
      SparkSession.builder
        .master("local[*]")
        .appName("spark test")
        .getOrCreate()

    }.bracket { implicit spark =>

      for {
        data <- IO(loadFromSettingsWithArgs(args))
        (events, purchases) = data
        _ <- IO(doJobs(events, purchases))
      } yield ()

    } { spark =>
      IO(spark.close())
    }

    program.unsafeRunSync()

  }
}
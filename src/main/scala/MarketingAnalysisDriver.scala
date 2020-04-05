package com.task

import cats.effect.IO
import cats.syntax.functor._
import com.task.infastructure.{DataLoader, WithJobs, WithSettings}
import org.apache.spark.sql.SparkSession

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
      IO(spark.close()).void
    }

    program.unsafeRunSync()

  }
}
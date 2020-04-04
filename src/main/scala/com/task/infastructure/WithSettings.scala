package com.task.infastructure

import com.typesafe.config.ConfigFactory
import pureconfig.ConfigSource

trait WithSettings {
  import pureconfig.generic.auto._
  val conf = ConfigFactory.defaultApplication().resolve()
  val settings = ConfigSource
    .fromConfig(conf)
    .at("analysis")
    .loadOrThrow[AppSettings]
}

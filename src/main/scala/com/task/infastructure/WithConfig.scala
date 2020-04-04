package com.task.infastructure

import com.typesafe.config.ConfigFactory

trait WithConfig {
  val conf = ConfigFactory.defaultApplication().resolve()
}

package com.metabolic.data.core.domain

import com.metabolic.data.core.services.util.DefaultsUtilsService
import com.typesafe.config.{Config => HoconConfig}

class Defaults(val config: HoconConfig) {

  def getDateTime(forKey: String) = DefaultsUtilsService.getDateTime(forKey, config)
  def getDate(forKey: String) = DefaultsUtilsService.getDate(forKey, config)
  def getPeriod(forKey: String) = DefaultsUtilsService.getPeriod(forKey, config)
}

object Defaults {
  def apply(config: HoconConfig) = new Defaults(config)
}
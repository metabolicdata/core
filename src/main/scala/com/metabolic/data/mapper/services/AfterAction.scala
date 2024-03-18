package com.metabolic.data.mapper.services

import com.metabolic.data.mapper.domain.Config

trait AfterAction {
  def name: String
  def run(config: Config): Unit
}

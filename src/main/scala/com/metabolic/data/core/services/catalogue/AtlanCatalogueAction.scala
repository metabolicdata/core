package com.metabolic.data.core.services.catalogue

import com.metabolic.data.mapper.domain.Config
import com.metabolic.data.mapper.domain.io.FileSink
import com.metabolic.data.mapper.services.AfterAction
import org.apache.logging.log4j.scala.Logging

class AtlanCatalogueAction extends AfterAction with Logging {

  override def name: String = "AtlanCatalogueAction"

  def run(config: Config): Unit = {

    config.sink match {
      case _: FileSink =>
        config.environment.atlanToken match {
          case Some(token) =>
            if (config.environment.atlanBaseUrlDataLake.isDefined && config.environment.atlanBaseUrlConfluent.isDefined) {
              val atlan = new AtlanService(token, config.environment.atlanBaseUrlDataLake.get, config.environment.atlanBaseUrlConfluent.get)
              atlan.setLineage(config)
              atlan.setMetadata(config)
              logger.info(s"After Action $name: Pushed lineage generated in ${config.name} to Atlan")
            }
            else
              logger.warn(s"After Action: Skipping $name for ${config.name} as Atlan Url is not provided")
          }
          case _ =>
            logger.warn(s"After Action: Skipping $name for ${config.name} as Atlan Token is not provided")
      case _ =>
        logger.warn(f"After Action: Skipping $name for ${config.name} as it is not a FileSink")

    }
  }
}

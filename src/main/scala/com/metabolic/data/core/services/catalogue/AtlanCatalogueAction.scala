package com.metabolic.data.core.services.catalogue

import com.metabolic.data.mapper.domain.Config
import com.metabolic.data.mapper.domain.io.FileSink
import com.metabolic.data.mapper.services.AfterAction
import org.apache.logging.log4j.scala.Logging

class AtlanCatalogueAction extends AfterAction with Logging {

  override def name: String = "AtlanCatalogueAction"

  def run(config: Config): Unit = {

    config.environment.atlanToken match {
      case Some(token) =>
        (config.environment.atlanBaseUrlDataLake, config.environment.atlanBaseUrlConfluent) match {
          case (Some(_), Some(_)) =>
            val atlan = new AtlanService(token, config.environment.atlanBaseUrlDataLake.get, config.environment.atlanBaseUrlConfluent.get)
            atlan.setLineage(config)
            atlan.setMetadata(config)
            logger.info(s"After Action $name: Pushed lineage generated in ${config.name} to Atlan")
          case _ =>
            logger.info(s"After Action: Skipping $name for ${config.name} as Atlan Url is not provided")
        }
      case None =>
        logger.info(s"After Action: Skipping $name for ${config.name} as Atlan Token is not provided")
    }
  }
}

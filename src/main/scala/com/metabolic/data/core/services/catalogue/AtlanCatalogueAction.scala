package com.metabolic.data.core.services.catalogue

import com.metabolic.data.mapper.domain.config.Config
import com.metabolic.data.mapper.domain.io.FileSink
import com.metabolic.data.mapper.services.AfterAction
import org.apache.logging.log4j.scala.Logging

class AtlanCatalogueAction extends AfterAction with Logging {

  override def name: String = "AtlanCatalogueAction"

  def run(config: Config): Unit = {

    val environment = config.metadata.environment

    environment.atlanToken match {
      case Some(token) =>
        (environment.atlanBaseUrlDataLake, environment.atlanBaseUrlConfluent) match {
          case (Some(_), Some(_)) =>
            val atlan = new AtlanService(token, environment.atlanBaseUrlDataLake.get, environment.atlanBaseUrlConfluent.get)
            atlan.setLineage(config)
            atlan.setOwner(config)
            atlan.setMetadata(config)
            atlan.setResource(config)
            logger.info(s"After Action $name: Pushed lineage generated in ${config.metadata.name} to Atlan")
          case _ =>
            logger.warn(s"After Action: Skipping $name for ${config.metadata.name} as Atlan Url is not provided")
        }
      case None =>
        logger.warn(s"After Action: Skipping $name for ${config.metadata.name} as Atlan Token is not provided")
    }
  }
}

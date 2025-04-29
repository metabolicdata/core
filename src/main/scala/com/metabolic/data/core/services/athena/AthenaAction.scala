package com.metabolic.data.core.services.athena

import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain.Config
import com.metabolic.data.mapper.domain.io.{FileSink, IOFormat}
import com.metabolic.data.mapper.services.AfterAction
import org.apache.logging.log4j.scala.Logging

class AthenaAction extends AfterAction with Logging {

  val name: String = "AthenaAction"

  def run(config: Config): Unit = {
    logger.info(f"Running After Action $name")

    val options = config.environment

    val region = options.region
    val dbName = options.dbName

    val athena = new AthenaCatalogueService()(region)

    config.sink match {
      case sink: FileSink =>
        sink.format match {
          case IOFormat.DELTA =>
            logger.info(f"After Action $name: Creating Delta Table for ${config.name}")
            val s3Path = sink.path.replaceAll("version=\\d+", "")
            val prefix =
              ConfigUtilsService.getTablePrefix(options.namespaces, s3Path)
            val tableName = prefix + ConfigUtilsService.getTableName(config)
            athena.dropView(dbName, tableName)
            athena.createDeltaTable(dbName, tableName, s3Path)

          case _ =>
            logger.info(f"After Action: Skipping $name for ${config.name} as it is not a DeltaSink")
        }
      case _ =>
        logger.info(f"After Action: Skipping $name for ${config.name} as it is not a FileSink")
    }

  }
}

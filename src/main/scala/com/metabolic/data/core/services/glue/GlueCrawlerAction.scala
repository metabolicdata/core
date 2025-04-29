package com.metabolic.data.core.services.glue

import com.metabolic.data.core.domain.Environment
import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain.Config
import com.metabolic.data.mapper.domain.io.{FileSink, IOFormat}
import com.metabolic.data.mapper.services.AfterAction
import org.apache.logging.log4j.scala.Logging


class GlueCrawlerAction extends AfterAction with Logging {

  override def name: String = "GlueCrawlerAction"

  override def run(config: Config): Unit = {

    val options = config.environment

    val region = options.region
    val crawlerName = s"${options.name} EM ${config.name}"

    val dbName = options.dbName
    val iamRole = options.iamRole

    val glue = new GlueCrawlerService()(region)

    config.sink match {
      case sink: FileSink =>
        sink.format match {
          case com.metabolic.data.mapper.domain.io.IOFormat.CSV =>
            runCrawler(config, options, crawlerName, dbName, iamRole, glue, sink)
          case com.metabolic.data.mapper.domain.io.IOFormat.PARQUET =>
            runCrawler(config, options, crawlerName, dbName, iamRole, glue, sink)
          case com.metabolic.data.mapper.domain.io.IOFormat.JSON =>
            runCrawler(config, options, crawlerName, dbName, iamRole, glue, sink)
          case com.metabolic.data.mapper.domain.io.IOFormat.DELTA =>
            logger.info(f"After Action $name: Skipping Glue Crawler for ${config.name} for DeltaSink")
          case com.metabolic.data.mapper.domain.io.IOFormat.DELTA_PARTITION =>
            logger.info(f"After Action $name: Skipping Glue Crawler for ${config.name} for DeltaPartitionSink")
          case com.metabolic.data.mapper.domain.io.IOFormat.KAFKA =>
            logger.info(f"After Action $name: Skipping Glue Crawler for ${config.name} for KafkaSink")
          case com.metabolic.data.mapper.domain.io.IOFormat.TABLE =>
            logger.info(f"After Action $name: Skipping Glue Crawler for ${config.name} for DeltaSink")
        }
      case _ =>
        logger.info(f"After Action: Skipping $crawlerName for ${config.name} as it is not a FileSink")
    }

  }

  private def runCrawler(config: Config, options: Environment, crawlerName: String, dbName: String, iamRole: String, glue: GlueCrawlerService, sink: FileSink): Unit = {
    val s3Path = sink.path.replaceAll("version=\\d+", "")
    val prefix = ConfigUtilsService.getTablePrefix(options.namespaces, s3Path)

    logger.info(f"After Action $name: Running Glue Crawler for ${config.name}")

    glue.createAndRunCrawler(iamRole, Seq(s3Path), dbName, crawlerName, prefix)
  }
}

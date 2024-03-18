package com.metabolic.data.core.services.glue

import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain.Config
import com.metabolic.data.mapper.domain.io.FileSink
import com.metabolic.data.mapper.services.AfterAction

class GlueCrawlerAction extends AfterAction {

  override def name: String = "GlueCrawlerAction"

  override def run(config: Config): Unit = {

    val options = config.environment

    val region = options.region
    val name = s"${options.name} EM ${config.name}"

    val dbName = options.dbName
    val iamRole = options.iamRole

    val glue = new GlueCrawlerService()(region)

    config.sink match {
      case sink: FileSink =>

        val s3Path = sink.path.replaceAll("version=\\d+", "")
        val prefix = ConfigUtilsService.getTablePrefix(options.namespaces, s3Path)

        glue.register(dbName, iamRole, name, Seq(s3Path), prefix)

    }

  }
}

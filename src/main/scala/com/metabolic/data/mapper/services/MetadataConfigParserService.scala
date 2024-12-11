package com.metabolic.data.mapper.services

import com.metabolic.data.core.domain.Environment
import com.metabolic.data.mapper.domain.config.Metadata
import com.typesafe.config.{Config => HoconConfig}

case class MetadataConfigParserService(platform: Environment) {

  def parseMetadata(config: HoconConfig): Metadata = {

    val name = parseName(config)

    val description = parseTableDescription(config)

    val owner = parseOwner(config)

    val sqlUrl = parseFileUrl(config, "sql")

    val confUrl = parseFileUrl(config, "conf")

    Metadata(name, description, owner, sqlUrl, confUrl, platform)

  }

  private def parseName(config: HoconConfig) = {
    if (config.hasPathOrNull("name")) {
      config.getString("name")
    } else {
      ""
    }
  }

  private def parseTableDescription(config: HoconConfig) = {
    if (config.hasPathOrNull("description")) {
      config.getString("description")
    } else {
      ""
    }
  }

  private def parseOwner(config: HoconConfig): String = {
    if (config.hasPathOrNull("owner")) {
      config.getString("owner")
    } else {
      ""
    }
  }

  private def parseFileUrl(config: HoconConfig, urlType: String): String = {
    val fileUrl = if (config.hasPath("mappings.file")) {
      config.getConfig("mappings").getString("file")
    } else if (config.hasPath("mapping.file")) {
      config.getConfig("mapping").getString("file")
    } else {
      ""
    }

    val mappingsBucket = System.getProperty("dp.mappings_bucket", "").replace("/mappings", "")
    val githubRepoUrl = System.getProperty("dp.github_repo_url", "")

    fileUrl.replace(".sql", s".$urlType").replace(mappingsBucket, githubRepoUrl)
  }

}

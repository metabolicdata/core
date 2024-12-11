package com.metabolic.data.mapper.services

import com.amazonaws.regions.Regions
import com.metabolic.data.core.domain.Environment
import com.metabolic.data.core.services.util.SecretsManagerService
import com.metabolic.data.mapper.domain.config.Config
import com.metabolic.data.mapper.domain.ops._
import com.metabolic.data.mapper.domain.run.EngineMode
import com.typesafe.config.{Config => HoconConfig}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._

class ConfigParserService(implicit region: Regions) extends Logging {

  def parseConfig(config: HoconConfig): Seq[Config] = {

    val environment = parseEnvironment(config.getConfig("dp"))

    val parsedConfigs = if( config.hasPath("entities")) {
      parseMultipleMappingConfig(config.getConfigList("entities").asScala, environment)
    } else {
      parseSimpleMappingConfig(config, environment)
    }

    parsedConfigs

  }

  private def parseEnvironment(config: HoconConfig) = {
    val envPrefix = config.getString("envPrefix")
    val dbname = config.getString("database")
    val iamrole = config.getString("iamrole")
    val crawl = config.getBoolean("crawl")

    val baseCheckpointLocation = if (config.hasPathOrNull("checkpointLocation")) {
      config.getString("checkpointLocation")
    } else {
      ""
    }

    val historical = if (config.hasPathOrNull("historical")){
      config.getBoolean("historical")
    } else {
      false
    }

    val engineMode = if (config.hasPathOrNull("stream")) {
      config.getBoolean("stream") match {
        case true   => EngineMode.Stream
        case false  => EngineMode.Batch
      }
    } else {
      EngineMode.Batch
    }

    val autoSchema = if (config.hasPathOrNull("autoSchema")){
      config.getBoolean("autoSchema")
    } else {
      false
    }

    val namespaces = if (config.hasPathOrNull("namespaces")){
      logger.info("Parsing namespaces")
      config.getString("namespaces").split(",").toSeq
    } else {
      Seq.empty
    }
    logger.info(f"Namespaces: $namespaces")

    val infix_namespaces = if (config.hasPathOrNull("infix_namespaces")) {
      config.getString("infix_namespaces").split(",").toSeq
    } else {
      Seq.empty
    }

    val enableJDBC = if (config.hasPathOrNull("enableJDBC")) {
      config.getBoolean("enableJDBC")
    } else {
      false
    }

    val queryOutputLocation = if (config.hasPathOrNull("queryOutputLocation")) {
      config.getString("queryOutputLocation")
    } else {
      ""
    }

    val region = if (config.hasPathOrNull("region")) {
      Regions.fromName(config.getString("region"))
    } else {
      Regions.fromName("eu-central-1")
    }

    var atlanToken: Option[String] = None
    var atlanBaseUrlDataLake: Option[String] = None
    var atlanBaseUrlConfluent: Option[String] = None

    val atlan = if (config.hasPathOrNull("atlan")){
      Option.apply(config.getString("atlan"))
    } else {
      Option.empty
    }
    if (atlan.isDefined && atlan.get.nonEmpty) {

      val secrets = new SecretsManagerService()(region)
      val kafkaSecretValue = secrets.get(atlan.get)
      val kafkaConfig = secrets.parseDict[AtlanConnection](kafkaSecretValue)

      atlanToken = kafkaConfig.atlan_token
      atlanBaseUrlDataLake = kafkaConfig.atlan_url_data_lake
      atlanBaseUrlConfluent = kafkaConfig.atlan_url_confluent

    }
    Environment(envPrefix, engineMode, baseCheckpointLocation, crawl, dbname, iamrole, region, atlanToken, atlanBaseUrlDataLake, atlanBaseUrlConfluent, historical, autoSchema, namespaces, infix_namespaces, enableJDBC, queryOutputLocation)
  }


  private def parseMultipleMappingConfig(values: Seq[_ <: HoconConfig], platform: Environment): Seq[Config] = {

    values.flatMap(parseSimpleMappingConfig(_, platform))

  }

  private def parseSimpleMappingConfig(config: HoconConfig, platform: Environment): Seq[Config] = {

    val sources = SourceConfigParserService().parseSources(config)

    val mappings = MappingConfigParserService().parseMappings(config)

    val sink = SinkConfigParserService().parseSink(config, platform)

    val metadata = MetadataConfigParserService(platform).parseMetadata(config)

    val parsedConfig = Config(sources, mappings, sink, metadata)

    Seq(parsedConfig)
  }




}

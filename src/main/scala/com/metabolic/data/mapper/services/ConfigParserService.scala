package com.metabolic.data.mapper.services

import com.amazonaws.regions.Regions
import com.metabolic.data.core.domain.{Defaults, Environment}
import com.metabolic.data.core.services.util.SecretsManagerService
import com.metabolic.data.mapper.domain._
import com.metabolic.data.mapper.domain.io.EngineMode
import com.metabolic.data.mapper.domain.ops._
import com.metabolic.data.mapper.domain.ops.mapping.{OpMapping, TupletIntervalMapping}
import com.typesafe.config.{Config => HoconConfig}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class ConfigParserService(implicit region: Regions) extends Logging {

  def parseConfig(config: HoconConfig): Seq[Config] = {

    val defaults = parseDefaults(config.getConfig("df"))
    val environment = parseEnvironment(config.getConfig("dp"))

    val parsedConfigs = if( config.hasPath("entities")) {
      parseMultipleMappingConfig(config.getConfigList("entities").asScala, defaults, environment)
    } else {
      parseSimpleMappingConfig(config, defaults, environment)
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

  private def parseDefaults(config: HoconConfig) = {
    new Defaults(config)
  }

  private def parseMultipleMappingConfig(values: Seq[_ <: HoconConfig], defaults: Defaults, platform: Environment): Seq[Config] = {

    values.flatMap(parseSimpleMappingConfig(_, defaults, platform))

  }

  private def parseSimpleMappingConfig(config: HoconConfig, defaults: Defaults, platform: Environment): Seq[Config] = {

    val name = parseName(config)

    val sources = SourceConfigParserService().parseSources(config)

    val mappings = parseMappings(config)

    val sink = SinkConfigParserService().parseSink(config, platform)

    Seq(new Config(name, sources, mappings, sink, defaults, platform))
  }

  private def parseName(config: HoconConfig) = {
    val mappingName = config.getString("name")

    mappingName
  }


  def parseIntervalOp(preOp: HoconConfig): Option[OpMapping] = {

    val leftTableName         = preOp.getString("leftTableName")
    val rightTableName        = preOp.getString("rightTableName")
    val leftIdColumnName      = preOp.getString("leftIdColumnName")
    val rightIdColumnName     = preOp.getString("rightIdColumnName")
    val leftWindowColumnName  = preOp.getString("leftWindowColumnName")
    val rightWindowColumnName = preOp.getString("rightWindowColumnName")
    val result = preOp.getString("result")

    Option.apply(
      TupletIntervalMapping(leftTableName, rightTableName,
        leftIdColumnName, rightIdColumnName, leftWindowColumnName,
        rightWindowColumnName, result )
    )

  }

  def parse(name: String, config: HoconConfig): Option[OpMapping] = {
    name match {
      case "intervals"  => parseIntervalOp(config)
      case _         => Option.empty
    }
  }

  def parseOps(preOpsConfigs: Seq[HoconConfig]): Seq[OpMapping] = {
    preOpsConfigs.flatMap { c =>
      val name = c.getString("op")
      parse(name, c)
    }
  }

  def checkMapping(mapping: HoconConfig): Seq[Mapping] = {

    val preOps = if(mapping.hasPathOrNull("preOps")) {

      parseOps(mapping.getConfigList("preOps").asScala)

    } else { Seq() }

    val mappingOps = if(mapping.hasPathOrNull("mapping")) {

      parseMappingType(mapping.getConfig("mapping"))

    } else { Seq() }

    val postOps = if(mapping.hasPathOrNull("postOps")) {

      parseOps(mapping.getConfigList("postOps").asScala)

    } else { Seq() }

    val mappings = Seq[Seq[Mapping]](preOps, mappingOps, postOps)
    mappings.flatten

  }

  def parseMappings(config: HoconConfig): Seq[Mapping] = {

    if(config.hasPathOrNull("mappings")) {
      checkMapping(config.getConfig("mappings"))
    } else if (config.hasPathOrNull("mapping")) {
      parseMappingType(config.getConfig("mapping"))
    } else  { Seq()}
  }

  private def parseMappingType(mappingConfig: HoconConfig): Seq[SQLMapping] = {
    val mapping = if (mappingConfig.hasPath("file")) {
      val fileLocation = mappingConfig.getString("file")
      new SQLFileMapping(fileLocation, region)

    } else if (mappingConfig.hasPath("sql")) {
      val sqlContents = mappingConfig.getString("sql")
      SQLStatmentMapping(sqlContents)

    } else {
      SQLStatmentMapping("select * from {MISSING_SOURCE}")
    }

    Seq(mapping)
  }

}

package com.metabolic.data.mapper.services

import com.amazonaws.regions.Regions
import com.metabolic.data.mapper.domain.ops.mapping.{OpMapping, TupletIntervalMapping}
import com.metabolic.data.mapper.domain.ops.{Mapping, SQLFileMapping, SQLMapping, SQLStatmentMapping}
import com.typesafe.config.{Config => HoconConfig}

import scala.collection.JavaConverters._

case class MappingConfigParserService(implicit region: Regions) {

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



}

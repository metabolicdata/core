package com.metabolic.data.mapper.services

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.util.SecretsManagerService
import com.metabolic.data.mapper.domain.KafkaConnection
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops.SourceOp
import com.typesafe.config.{Config => HoconConfig}
import org.apache.logging.log4j.scala.Logging


case class SourceFormatParser()(implicit val region: Regions) extends FormatParser with Logging {

  def parse(name: String, config: HoconConfig, ops: Seq[SourceOp]): Source = {

    val format = parseFormat(config)

    format match {
      case IOFormat.CSV => parseCSVSource(name, config, ops)
      case IOFormat.DELTA => parseDeltaSource(name, config, ops)
      case IOFormat.JSON => parseJsonSource(name, config, ops)
      case IOFormat.PARQUET => parseParquetSource(name, config, ops)

      case IOFormat.KAFKA => parseKafkaSource(name, config, ops)

      case IOFormat.TABLE => parseMetastoreSource(name, config, ops)
    }
  }

  private def parseDeltaSource(name: String, config: HoconConfig, ops: Seq[SourceOp]): Source = {
    val path = if(config.hasPathOrNull("inputPath")) { config.getString("inputPath")}
    else { config.getString("path") }
    val startingTime = if(config.hasPathOrNull("startTime")){Option.apply(config.getString("startingTime"))}
    else{Option.empty}
    FileSource(path, name, IOFormat.DELTA, false, ops, startingTime)
  }

  private def parseJsonSource(name: String, config: HoconConfig, ops: Seq[SourceOp]): Source = {
    val path = if(config.hasPathOrNull("inputPath")) { config.getString("inputPath")}
    else { config.getString("path") }

    val useStringPrimitives = if(config.hasPathOrNull("useStringPrimitives")) { config.getBoolean("useStringPrimitives")}
    else { false }

    FileSource(path, name, IOFormat.JSON, useStringPrimitives, ops, Option.empty)
  }

  private def parseCSVSource(name: String, config: HoconConfig, ops: Seq[SourceOp]): Source = {
    val path = if(config.hasPathOrNull("inputPath")) { config.getString("inputPath")}
    else { config.getString("path") }

    val useStringPrimitives = if(config.hasPathOrNull("useStringPrimitives")) { config.getBoolean("useStringPrimitives")}
    else { false }

    FileSource(path, name, IOFormat.CSV, useStringPrimitives, ops, Option.empty)
  }

  private def parseParquetSource(name: String, config: HoconConfig, ops: Seq[SourceOp]): Source = {
    val path = if(config.hasPathOrNull("inputPath")) { config.getString("inputPath")}
    else { config.getString("path") }
    
    FileSource(path, name, IOFormat.PARQUET, false, ops, Option.empty)
  }

  private def parseKafkaSource(name: String, config: HoconConfig, ops: Seq[SourceOp]): Source = {

    val kafkaSecretKey = config.getString("kafkaSecret")

    val secrets = new SecretsManagerService()
    val kafkaSecretValue = secrets.get(kafkaSecretKey)

    val kafkaConfig = secrets.parseDict[KafkaConnection](kafkaSecretValue)

    val servers = kafkaConfig.servers.get
    val apiKey = kafkaConfig.key.get
    val apiSecret = kafkaConfig.secret.get

    val topic = config.getString("topic")

    StreamSource(name, servers, apiKey, apiSecret, topic, IOFormat.KAFKA, ops)
  }

  private def parseMetastoreSource(name: String, config: HoconConfig, ops: Seq[SourceOp]): Source = {
    val fqdn = config.getString("catalog")

    MetastoreSource(fqdn, name, ops)
  }

}

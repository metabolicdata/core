package com.metabolic.data.mapper.services

import com.amazonaws.regions.Regions
import com.metabolic.data.core.domain.{Environment, Platform}
import com.metabolic.data.core.services.util.SecretsManagerService
import com.metabolic.data.mapper.domain.KafkaConnection
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops.SinkOp
import com.typesafe.config.{Config => HoconConfig}
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime

import scala.collection.JavaConverters._

case class SinkFormatParser()(implicit val region: Regions) extends FormatParser {

  def parse(name: String , config: HoconConfig, ops: Seq[SinkOp], platform: Environment): Sink = {

    val format = parseFormat(config)

    format match {
      case IOFormat.CSV => parseCSVSink(name, config, ops)
      case IOFormat.DELTA => parseDeltaSink(name, config, ops, platform)
      case IOFormat.JSON => parseJsonSink(name, config, ops)
      case IOFormat.PARQUET => parseParquetSink(name, config, ops)
      case IOFormat.KAFKA => parseKafkaSink(name, config, ops)
    }
  }

  private def parseDeltaSink(name: String, config: HoconConfig, ops: Seq[SinkOp], platform: Environment): Sink = {
    val path = if(config.hasPathOrNull("outputPath")) { config.getString("outputPath")}
               else { config.getString("path") }

    val idColumnName = if(config.hasPath("idColumn")) {
      Option(config.getString("idColumn"))
    } else None

    val eventTimeColumnName = if(config.hasPath("eventDtColumn")) {
      Option(config.getString("eventDtColumn"))
    } else None

    val processingTimeColumnName = DateTime.now().toString

    val writeMode = checkWriteMode(config)
    val upsert = checkUpsert(config)
    val partitionCols = checkPartitionCols(config)
    val dbName = platform.dbName
    val checkpointLocation = if (config.hasPath("checkpointLocation")) {
      Option(config.getString("checkpointLocation"))
    } else None

    FileSink(name, path, writeMode, IOFormat.DELTA, idColumnName, eventTimeColumnName,
      processingTimeColumnName=processingTimeColumnName, partitionColumnNames = partitionCols, upsert = upsert, ops = ops,
      checkpointLocation = checkpointLocation, dbName = dbName )
  }

  private def parseJsonSink(name: String, config: HoconConfig, ops: Seq[SinkOp]): Sink = {
    val path = if(config.hasPathOrNull("outputPath")) { config.getString("outputPath")}
    else { config.getString("path") }

    val eventTimeColumnName = if(config.hasPath("eventDtColumn")) {
      Option(config.getString("eventDtColumn"))
    } else None

    val processingTimeColumnName = DateTime.now().toString

    val writeMode = checkWriteMode(config)
    val partitionCols = checkPartitionCols(config)

    FileSink(name, path, writeMode, IOFormat.JSON, eventTimeColumnName = eventTimeColumnName,
      processingTimeColumnName = processingTimeColumnName, partitionColumnNames = partitionCols, ops = ops )
  }

  private def parseCSVSink(name: String, config: HoconConfig, ops: Seq[SinkOp]): Sink = {
    val path = if(config.hasPathOrNull("outputPath")) { config.getString("outputPath")}
    else { config.getString("path") }

    val eventTimeColumnName = if(config.hasPath("eventDtColumn")) {
      Option(config.getString("eventDtColumn"))
    } else None

    val processingTimeColumnName = DateTime.now().toString

    val writeMode = checkWriteMode(config)
    val partitionCols = checkPartitionCols(config)

    FileSink(name, path, writeMode, IOFormat.CSV, eventTimeColumnName = eventTimeColumnName,
      processingTimeColumnName = processingTimeColumnName, partitionColumnNames = partitionCols, ops = ops )
  }

  private def parseParquetSink(name: String, config: HoconConfig, ops: Seq[SinkOp]): Sink = {

    val path = if(config.hasPathOrNull("outputPath")) { config.getString("outputPath")}
    else { config.getString("path") }

    val eventTimeColumnName = if(config.hasPath("eventDtColumn")) {
      Option(config.getString("eventDtColumn"))
    } else None

    val processingTimeColumnName = DateTime.now().toString

    val writeMode = checkWriteMode(config)
    val partitionCols = checkPartitionCols(config)

    FileSink(name, path, writeMode, IOFormat.PARQUET, eventTimeColumnName = eventTimeColumnName,
      processingTimeColumnName = processingTimeColumnName, partitionColumnNames = partitionCols, ops = ops )
  }

  private def parseKafkaSink(name: String, config: HoconConfig, ops: Seq[SinkOp]): Sink = {
    val kafkaSecretKey = config.getString("kafkaSecret")

    val secrets = new SecretsManagerService()
    val kafkaSecretValue = secrets.get(kafkaSecretKey)

    val kafkaConfig = secrets.parseDict[KafkaConnection](kafkaSecretValue)

    val servers = kafkaConfig.servers.get
    val apiKey = kafkaConfig.key.get
    val apiSecret = kafkaConfig.secret.get

    val topic = config.getString("topic")

    val idColumnName = if(config.hasPathOrNull("idColumn")){
      Option(config.getString("idColumn"))
    } else {
      Option.empty
    }

    StreamSink(name, servers, apiKey, apiSecret, topic, idColumnName, IOFormat.KAFKA, ops = ops )
  }

  private def checkWriteMode(config: HoconConfig): WriteMode = {
    if (config.hasPathOrNull("writeMode")) {
      parseWriteMode(config.getString("writeMode"))
    } else {
      WriteMode.Append
    }
  }

  private def parseWriteMode(contents: String): WriteMode = {
    val cleanContents = contents.toLowerCase.trim
    WriteMode.withName(cleanContents)
  }

  private def checkUpsert(config: HoconConfig): Boolean = {
    if (config.hasPathOrNull("writeMode")) {
      parseUpsert(config.getString("writeMode"))
    } else {
      false
    }
  }

  private def parseUpsert(contents: String): Boolean = {
    val cleanContents = contents.toLowerCase.trim
    cleanContents match {
      case "upsert" => true
      case _ => false
    }
  }

  def checkPartitionCols(config: HoconConfig): Seq[String] = {
    if (config.hasPathOrNull("addPartitionCols")) {
      config.getStringList("addPartitionCols").asScala.toSeq
    } else {
      Seq.empty
    }
  }

}

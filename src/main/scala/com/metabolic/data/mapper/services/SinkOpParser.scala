package com.metabolic.data.mapper.services

import com.metabolic.data.core.services.spark.filter.DataLakeDepth
import com.metabolic.data.mapper.domain.ops.SinkOp
import com.metabolic.data.mapper.domain.ops.sink._
import com.typesafe.config.{Config => HoconConfig}
import org.apache.spark.sql.SaveMode

import scala.collection.JavaConverters._

object SinkOpParser {

  def parse(name: String, config: HoconConfig): Option[SinkOp]= {
    name match {
      case "date_partition" => parseDatePartitionSinkOp(config)
      case "cols_partition" => parseColPartitionSinkOp(config)
      case "manage_schema" => parseManageSchemaSinkOp(config)
      case "flatten" => parseFlatenSinkOp(config)
      case _ => Option.empty
    }
  }

  def parseColPartitionSinkOp(config: HoconConfig): Option[ExplicitPartitionSinkOp] = {

    val partitionColumns = config.getStringList("cols").asScala

    Option.apply(ExplicitPartitionSinkOp(partitionColumns))
  }

  def parseDatePartitionSinkOp(config: HoconConfig): Option[DatePartitionSinkOp] = {

    val partitionColumn = if(config.hasPathOrNull("col")) {
      config.getString("col")
    } else if (config.hasPathOrNull("eventDtColumn")) {
      config.getString("eventDtColumn")
    } else { "" }

    val depth = parseDepth(config)

    Option.apply(DatePartitionSinkOp(partitionColumn, depth))
  }

  def parseDepth(config: HoconConfig): DataLakeDepth.DataLakeDepth = {
    if (config.hasPathOrNull("depth")) {
      val depthString = config.getString("depth").toLowerCase.trim
      try {
        DataLakeDepth.withName(depthString)
      } catch {
        case _: NoSuchElementException =>
          throw new UnsupportedOperationException(s"$depthString is not supported. Available depths are ${DataLakeDepth.values}")
      }
    } else {
      DataLakeDepth.DAY
    }

  }

  def parseManageSchemaSinkOp(config: HoconConfig): Option[ManageSchemaSinkOp] = {
    Option.apply(ManageSchemaSinkOp())
  }


  private def checkSaveMode(config: HoconConfig): SaveMode = {
    if (config.hasPathOrNull("writeMode")) {
      parseSaveMode(config.getString("writeMode"))
    } else {
      SaveMode.Append
    }
  }

  private def parseSaveMode(contents: String): SaveMode = {
    val cleanContents = contents.toLowerCase.trim
    cleanContents match {
      case "replace" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case _ => SaveMode.Append
    }
  }

  def parseFlatenSinkOp(config: HoconConfig): Option[FlattenSinkOp] = {

    val column = if(config.hasPathOrNull("column")) {
      Option.apply(config.getString("column"))
    } else {
      Option.empty
    }
    Option.apply(FlattenSinkOp(column))

  }

}

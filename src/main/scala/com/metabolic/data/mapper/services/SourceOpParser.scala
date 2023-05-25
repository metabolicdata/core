package com.metabolic.data.mapper.services

import com.metabolic.data.mapper.domain.ops.source.SQLOrder.SQLOrder
import com.metabolic.data.mapper.domain.ops._
import com.metabolic.data.mapper.domain.ops.source._
import com.typesafe.config.{Config => HoconConfig}

import scala.collection.JavaConverters._

object SourceOpParser {

  def parse(name: String, config: HoconConfig): Option[SourceOp] = {
    name match {
      case "filter"     => parseFilterSourceOp(config)
      case "prune"      => parsePruneSourceOp(config)
      case "dedupe"     => parseDedupeSourceOp(config)
      case "demulti"    => parseDemultiSourceOp(config)
      case "expr"       => parseSelectExpressionSourceOp(config)
      case "drop"       => parseDropExpressionSourceOp(config)
      case "flatten"    => parseFlatenSourceOp(config)
      case "watermark"  => parseWatermarkSourceOp(config)
      case _            => Option.empty
    }
  }

  def parseFilterSourceOp(config: HoconConfig): Option[FilterSourceOp] = {

    val onColumn = config.getString("onColumn")
    val from = config.getString("from")
    val to = config.getString("to")

    Option.apply(FilterSourceOp(onColumn, from, to))
  }

  def parsePruneSourceOp(config: HoconConfig): Option[PruneDateComponentsSourceOp] = {

    val from = config.getString("from")
    val to = config.getString("to")

    Option.apply(PruneDateComponentsSourceOp(from, to))
  }

  def parseWatermarkSourceOp(config: HoconConfig): Option[WatermarkSourceOp] = {

    val onColumn = config.getString("onColumn")
    val value = config.getString("value")

    Option.apply(WatermarkSourceOp(onColumn, value))
  }

  def parseOrder(orderString: String): SQLOrder = {
    SQLOrder.withName(orderString)
  }

  def parseDedupeSourceOp(config: HoconConfig): Option[DedupeSourceOp] = {

    val idColumns = config.getStringList("idColumns").asScala
    val orderColumns = config.getStringList("orderColumns").asScala
    if (config.hasPathOrNull("order")) {
      val order = parseOrder(config.getString("order"))
      Option.apply(DedupeSourceOp(idColumns, orderColumns, order))
    } else {
      Option.apply(DedupeSourceOp(idColumns, orderColumns))
    }

  }

  def parseSelectExpressionSourceOp(config: HoconConfig): Option[SelectExpressionSourceOp] = {

    val expressions = if(config.hasPathOrNull("expressions")) {
      config.getStringList("expressions").asScala
    } else {
      Seq(config.getString("expression"))
    }

    Option.apply(SelectExpressionSourceOp(expressions))
  }

  def parseDropExpressionSourceOp(config: HoconConfig): Option[DropExpressionSourceOp] = {

    val columns = if(config.hasPathOrNull("columns")) {
      config.getStringList("columns").asScala
    } else {
      Seq(config.getString("column"))
    }

    Option.apply(DropExpressionSourceOp(columns))
  }

  def parseFlatenSourceOp(config: HoconConfig): Option[FlattenSourceOp] = {

    val column = if (config.hasPathOrNull("column")) {
      Option.apply(config.getString("column"))
    } else {
      Option.empty
    }

    Option.apply(FlattenSourceOp(column))

  }

  def parseDemultiSourceOp(config: HoconConfig): Option[DemultiSourceOp] = {

    val idColumns = config.getStringList("idColumns").asScala
    val orderColumns = config.getStringList("orderColumns").asScala
    val dateColumn = config.getString("dateColumn")

    val periodFormat = config.getString("format")

    val from = config.getString("from")
    val to = if(config.hasPathOrNull("to")) {
      Option.apply(config.getString("to"))
    } else { Option.empty }

    val endOfMonth = if(config.hasPathOrNull("endOfMonth")) {
      Option.apply(config.getBoolean("endOfMonth"))
    } else { Option.empty }

    Option.apply(new DemultiSourceOp(idColumns, orderColumns, dateColumn, periodFormat, from, to, endOfMonth))
  }

}
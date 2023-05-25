package com.metabolic.data.mapper.services

import com.amazonaws.regions.Regions
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops.SourceOp
import com.typesafe.config.{Config => HoconConfig}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._

case class SourceConfigParserService()(implicit val region: Regions) extends Logging {

  def parseSources(config: HoconConfig): Seq[Source] = {

    val sourcesList = config.getConfigList("sources").asScala.toSeq

    sourcesList.map { source =>
      checkSource(source)
    }
  }

  private def checkSource(source: HoconConfig): Source = {

    val name = source.getString("name")

    val ops = checkOps(source)

    SourceFormatParser().parse(name, source, ops)

  }

  private def checkOps(source: HoconConfig): Seq[SourceOp] = {
    if (source.hasPathOrNull("ops")) {
      parseOps(source.getConfigList("ops").asScala)
    } else {
      parseOps(source)
    }
  }

  private def parseOps(sourceOps: Seq[HoconConfig]): Seq[SourceOp] = {

    sourceOps.map { sourceOp =>

      val key = sourceOp.getString("op")
      SourceOpParser.parse(key, sourceOp)

    }.flatten

  }

  /*
      Alternative as op.filter = { foo } op.dedupe = { bar }
      Problem is, this is a set of op.* therefore order of execution is not guaranteed.
      Spark logical plan should take care of this but more reasoning is required.
   */
  private def parseOps(source: HoconConfig): Seq[SourceOp] = {
    if (source.hasPathOrNull("op")) {
      val opConfigs = source.getConfig("op")

      opConfigs.entrySet().asScala
        .map { entry =>
          entry.getKey.split('.').head
        }
        .foldLeft(Set[String]()) { (r, e) =>
          r ++ Set(e)
        }.map { key =>
        val opConfig = opConfigs.getConfig(key)
        SourceOpParser.parse(key, opConfig)
      }
        .flatMap { f => f }
        .toSeq
    } else {
      Seq.empty
    }
  }

}

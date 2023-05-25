package com.metabolic.data.mapper.services

import com.amazonaws.regions.Regions
import com.metabolic.data.core.domain.Environment
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops.SinkOp
import com.typesafe.config.{Config => HoconConfig}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._

case class SinkConfigParserService()(implicit val region: Regions) extends Logging {

    def parseSink(config: HoconConfig, platform: Environment): Sink = {

      val name = config.getString("name")
          .toLowerCase
          .replaceAll("\\W", "_")

      val sinkConfig = config.getConfig("sink")

      val ops = checkOps(sinkConfig)

      SinkFormatParser().parse(name, sinkConfig, ops, platform )
    }

    private def checkOps(source: HoconConfig): Seq[SinkOp] = {
        if (source.hasPathOrNull("ops")) {
            parseOps(source.getConfigList("ops").asScala)
        } else {
            parseOps(source)
        }
    }

    private def parseOps(sinkOps: Seq[HoconConfig]): Seq[SinkOp] = {

        sinkOps.map { sinkOp =>

            val key = sinkOp.getString("op")
            SinkOpParser.parse(key, sinkOp)

        }.flatten

    }

    private def parseOps(sink: HoconConfig): Seq[SinkOp] = {
        if (sink.hasPathOrNull("op")) {
            val opConfigs = sink.getConfig("op")

            opConfigs.entrySet().asScala
              .map { entry =>
                  entry.getKey.split('.').head
              }
              .foldLeft(Set[String]()) { (r, e) =>
                  r ++ Set(e)
              }.map { key =>
                val opConfig = opConfigs.getConfig(key)
                SinkOpParser.parse(key, opConfig)
            }
              .flatMap { f => f }
              .toSeq
        } else {
            Seq.empty
        }
    }

}

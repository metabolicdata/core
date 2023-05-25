package com.metabolic.data.mapper.domain

import com.metabolic.data.core.domain.{CoreConfig, Defaults, Environment}
import com.metabolic.data.mapper.domain.io.{EngineMode, Sink, Source}
import com.metabolic.data.mapper.domain.ops.Mapping
import com.typesafe.config.ConfigFactory

class Config(val name: String, val sources: Seq[Source], val mappings: Seq[Mapping], val sink: Sink,
             defaults: Defaults, environment: Environment) extends CoreConfig(defaults, environment) {

  def getName() = {
    name
  }

  def getSourcesNames(): String  = {
    sources.map( _.name ).mkString( " + " )
  }

}

object Config {

  def apply(name: String, sources: Seq[Source], mappings: Seq[Mapping], sink: Sink,
            defaults: Defaults, platform: Environment): Config = {
    new Config(name, sources, mappings, sink, defaults, platform)
  }

  def apply(name: String, sources: Seq[Source], mappings: Seq[Mapping], sink: Sink): Config = {
    val defaults: Defaults = Defaults(ConfigFactory.load())
    val environment: Environment = Environment("", EngineMode.Batch, "", false,"test","",Option.empty)
    new Config(name, sources, mappings, sink, defaults, environment)
  }
}

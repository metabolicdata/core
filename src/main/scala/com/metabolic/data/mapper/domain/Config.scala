package com.metabolic.data.mapper.domain

import com.amazonaws.regions.Regions
import com.metabolic.data.core.domain.{CoreConfig, Defaults, Environment}
import com.metabolic.data.mapper.domain.io.{EngineMode, Sink, Source}
import com.metabolic.data.mapper.domain.ops.Mapping
import com.typesafe.config.ConfigFactory

class Config(val name: String, val sources: Seq[Source], val mappings: Seq[Mapping], val sink: Sink,
             defaults: Defaults, environment: Environment, val owner: String="", val sqlUrl: String="", val confUrl: String="") extends CoreConfig(defaults, environment) {

  def getCanonicalName() = {
    //regex to remove all non-alphanumeric characters
    name
      .toLowerCase()
      .replaceAll(" ", "-")

  }

  def getSourcesNames(): String  = {
    sources.map( _.name ).mkString( " + " )
  }

}

object Config {

  def apply(name: String, sources: Seq[Source], mappings: Seq[Mapping], sink: Sink,
            defaults: Defaults, platform: Environment): Config = {
    new Config(name, sources, mappings, sink, defaults, platform, "", "", "")
  }

  def apply(name: String, sources: Seq[Source], mappings: Seq[Mapping], sink: Sink): Config = {
    val defaults: Defaults = Defaults(ConfigFactory.load())
    val environment: Environment = Environment("", EngineMode.Batch, "", false,"test","",
      Regions.fromName("eu-central-1"),Option.empty, Option.empty, Option.empty)
    new Config(name, sources, mappings, sink, defaults, environment, "", "", "")
  }

  def apply(name: String, sources: Seq[Source], mappings: Seq[Mapping], sink: Sink, owner: String, sqlUrl: String, confUrl: String): Config = {
    val defaults: Defaults = Defaults(ConfigFactory.load())
    val environment: Environment = Environment("", EngineMode.Batch, "", false,"test","",
      Regions.fromName("eu-central-1"),Option.empty, Option.empty, Option.empty)
    new Config(name, sources, mappings, sink, defaults, environment, owner, sqlUrl, confUrl)
  }
}

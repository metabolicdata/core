package com.metabolic.data.mapper.domain.config

import com.metabolic.data.mapper.domain.io.{Sink, Source}
import com.metabolic.data.mapper.domain.ops.Mapping

case class Config(sources: Seq[Source],
                  mappings: Seq[Mapping],
                  sink: Sink,
                  metadata: Metadata)

case object Config {

  def apply(sources: Seq[Source], mapping: Mapping, sink: Sink, metadata: Metadata): Config = {
    new Config(sources, Seq(mapping), sink, metadata)
  }

}

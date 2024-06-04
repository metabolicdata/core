package com.metabolic.data.mapper.domain.io

import IOFormat.{IOFormat, KAFKA}
import com.metabolic.data.mapper.domain.ops.SourceOp

case class StreamSource(name: String,
                        servers: Seq[String],
                        key: String,
                        secret: String,
                        topic: String,
                        schemaRegistryUrl: String,
                        srApiKey: String,
                        srApiSecret: String,
                        schemaRegistry: Option[String],
                        format: IOFormat = KAFKA,
                        ops: Seq[SourceOp] = Seq.empty)
  extends Source

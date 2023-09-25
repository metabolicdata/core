package com.metabolic.data.mapper.domain.io

import com.metabolic.data.mapper.domain.io.IOFormat.{IOFormat, KAFKA}
import com.metabolic.data.mapper.domain.ops.SinkOp

class KafkaStreamSink (name: String,
                       servers: Seq[String],
                       apiKey: String,
                       apiSecret: String,
                       topic: String,
                       idColumnName: Option[String],
                       val schemaRegistryUrl: String,
                       val srApiKey: String,
                       val srApiSecret: String,
                       val schemaRegistry: Option[String],
                       format: IOFormat = KAFKA,
                       ops: Seq[SinkOp])
  extends StreamSink(name,
    servers,
    apiKey,
    apiSecret,
    topic,
    idColumnName,
    format,
    ops) {

}
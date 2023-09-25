package com.metabolic.data.mapper.domain.io
import com.metabolic.data.mapper.domain.io.IOFormat.{IOFormat, KAFKA}
import com.metabolic.data.mapper.domain.ops.SourceOp

class KafkaStreamSource (name: String,
                         servers: Seq[String],
                         apiKey: String,
                         apiSecret: String,
                         topic: String,
                         val schemaRegistryUrl: String,
                         val srApiKey: String,
                         val srApiSecret: String,
                         val schemaRegistry: Option[String],
                         format: IOFormat = KAFKA,
                         ops: Seq[SourceOp])
  extends StreamSource(name,
    servers,
    apiKey,
    apiSecret,
    topic,
    format,
    ops){

}

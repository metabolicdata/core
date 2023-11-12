package com.metabolic.data.mapper.domain.io

import IOFormat.{IOFormat, KAFKA}
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import com.metabolic.data.mapper.domain.ops.SinkOp
import org.apache.spark.sql.SaveMode

case class StreamSink(name: String,
                      servers: Seq[String],
                      apiKey: String,
                      apiSecret: String,
                      topic: String,
                      idColumnName: Option[String],
                      format: IOFormat = KAFKA,
                      ops: Seq[SinkOp])
  extends Sink {

  override def writeMode: WriteMode = WriteMode.Append

  override def partitionColumnNames: Seq[String] = Seq.empty

}

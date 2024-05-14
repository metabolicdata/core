package com.metabolic.data.mapper.domain.io

import com.metabolic.data.mapper.domain.io.IOFormat.{DELTA, IOFormat}
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import com.metabolic.data.mapper.domain.ops.SinkOp

case class FileSink(name: String,
                    path: String,
                    writeMode: WriteMode,
                    format: IOFormat = DELTA,
                    idColumnName: Option[String] = None,
                    eventTimeColumnName: Option[String] = None,
                    upsert: Boolean = false,
                    processingTimeColumnName: String = "",
                    partitionColumnNames: Seq[String] = Seq.empty,
                    ops: Seq[SinkOp] = Seq.empty,
                    checkpointLocation: Option[String] = None,
                    dbName: String = "",
                    optimize: Option[Boolean] = None,
                    optimizeEvery: Option[Int] = None
                   )
  extends Sink

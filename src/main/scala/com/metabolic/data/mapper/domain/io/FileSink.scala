package com.metabolic.data.mapper.domain.io

import IOFormat.{DELTA, IOFormat}
import com.metabolic.data.mapper.domain.ops.SinkOp
import org.apache.spark.sql.SaveMode

case class FileSink(name: String,
                    path: String,
                    saveMode: SaveMode,
                    format: IOFormat = DELTA,
                    idColumnName: Option[String] = None,
                    eventTimeColumnName: Option[String] = None,
                    upsert: Boolean = false,
                    processingTimeColumnName: String = "",
                    partitionColumnNames: Seq[String] = Seq.empty,
                    ops: Seq[SinkOp] = Seq.empty,
                    checkpointLocation: Option[String] = None,
                    dbName: String = ""
                   )
  extends Sink

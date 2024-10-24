package com.metabolic.data.mapper.domain.io

import com.metabolic.data.mapper.domain.io.IOFormat.{IOFormat, TABLE}
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import com.metabolic.data.mapper.domain.ops.SinkOp

case class TableSink(name: String,
                     catalog: String,
                     writeMode: WriteMode,
                     ops: Seq[SinkOp])
  extends Sink {

  val format: IOFormat = TABLE
  override def partitionColumnNames: Seq[String] = Seq.empty

}

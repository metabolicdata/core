package com.metabolic.data.mapper.domain.io

import IOFormat.IOFormat
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import com.metabolic.data.mapper.domain.ops.{SinkOp, SourceOp}
import org.apache.spark.sql.SaveMode

trait Sink {
  def partitionColumnNames: Seq[String]

  def name: String

  def format: IOFormat

  def writeMode: WriteMode

  def ops: Seq[SinkOp]

}

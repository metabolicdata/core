package com.metabolic.data.mapper.domain.ops.sink

import com.metabolic.data.core.services.spark.filter.DataLakeDepth.{DAY, DataLakeDepth}
import com.metabolic.data.mapper.domain.ops.SinkOp

class DatePartitionSinkOp(
                       val eventTimeColumnName: String,
                       val depth: DataLakeDepth,
                     ) extends SinkOp {
  override def opName: String = "date_partition"
}

object DatePartitionSinkOp {
  def apply( eventTimeColumnName: String,
             depth: DataLakeDepth = DAY
           ): DatePartitionSinkOp = {
    new DatePartitionSinkOp(eventTimeColumnName, depth)
  }
}
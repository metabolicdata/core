package com.metabolic.data.mapper.domain.ops.sink

import com.metabolic.data.mapper.domain.ops.SinkOp

case class ExplicitPartitionSinkOp( partitionColumns: Seq[String]
                     ) extends SinkOp {
  override def opName: String = "explicit_partition"
}

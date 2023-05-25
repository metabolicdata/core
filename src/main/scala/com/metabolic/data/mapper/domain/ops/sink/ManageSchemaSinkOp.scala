package com.metabolic.data.mapper.domain.ops.sink

import com.metabolic.data.mapper.domain.ops.SinkOp

case class ManageSchemaSinkOp() extends SinkOp {
  override def opName: String = "manage_schema"
}
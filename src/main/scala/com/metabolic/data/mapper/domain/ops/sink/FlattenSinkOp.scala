package com.metabolic.data.mapper.domain.ops.sink

import com.metabolic.data.mapper.domain.ops.SinkOp

case class FlattenSinkOp(column: Option[String]) extends SinkOp {

    val opName = "flatten"

}

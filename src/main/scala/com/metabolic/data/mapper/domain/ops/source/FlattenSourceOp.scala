package com.metabolic.data.mapper.domain.ops.source

import com.metabolic.data.mapper.domain.ops.SourceOp

case class FlattenSourceOp(column: Option[String]) extends SourceOp {

    val opName = "flatten"

}

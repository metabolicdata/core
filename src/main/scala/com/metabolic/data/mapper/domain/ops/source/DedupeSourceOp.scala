package com.metabolic.data.mapper.domain.ops.source

import com.metabolic.data.mapper.domain.ops.SourceOp
import com.metabolic.data.mapper.domain.ops.source.SQLOrder.SQLOrder

class DedupeSourceOp(
                      val idColumns: Seq[String],
                      val orderColumns: Seq[String],
                      val order: SQLOrder,
                    ) extends SourceOp {

  val opName = "dedupe"

}

object DedupeSourceOp {

  def apply( idColumns: Seq[String], orderColumns: Seq[String], order: SQLOrder = SQLOrder.Descending): DedupeSourceOp = {
    new DedupeSourceOp(idColumns, orderColumns, order)
  }

}

package com.metabolic.data.mapper.domain.ops.source

import com.metabolic.data.mapper.domain.ops.SourceOp

class DropExpressionSourceOp(
                      val columns: Seq[String]
                    ) extends SourceOp {

    val opName = "drop"

}


object DropExpressionSourceOp {

  def apply(columns: Seq[String]): DropExpressionSourceOp = {
    new DropExpressionSourceOp(columns)
  }

}


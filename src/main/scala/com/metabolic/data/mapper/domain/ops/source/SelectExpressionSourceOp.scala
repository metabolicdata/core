package com.metabolic.data.mapper.domain.ops.source

import com.metabolic.data.mapper.domain.ops.SourceOp

class SelectExpressionSourceOp(
                      val expressions: Seq[String]
                    ) extends SourceOp {

    val opName = "expr"

}

object SelectExpressionSourceOp {

  def apply(expressions: Seq[String]): SelectExpressionSourceOp = {
    new SelectExpressionSourceOp(expressions)
  }

}

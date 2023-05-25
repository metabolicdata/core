package com.metabolic.data.mapper.domain.ops.source

import com.metabolic.data.mapper.domain.ops.SourceOp


class WatermarkSourceOp(
                      val onColumn: String,
                      val value: String
                    ) extends SourceOp {

  val opName = "watermark"

}

object WatermarkSourceOp {

  def apply(onColumn: String, value: String): WatermarkSourceOp = {
    new WatermarkSourceOp(onColumn, value)
  }
}
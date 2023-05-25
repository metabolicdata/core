package com.metabolic.data.mapper.domain.io

import com.metabolic.data.mapper.domain.ops.SourceOp

trait Source {
  def name: String

  def ops: Seq[SourceOp]
}

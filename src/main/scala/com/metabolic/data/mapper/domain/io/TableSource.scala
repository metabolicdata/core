package com.metabolic.data.mapper.domain.io

import com.metabolic.data.mapper.domain.ops.SourceOp

case class TableSource(
                            fqn: String,
                            name: String,
                            ops: Seq[SourceOp] = Seq.empty
                          ) extends Source

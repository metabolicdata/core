package com.metabolic.data.mapper.domain.io

import IOFormat.{DELTA, IOFormat}
import com.metabolic.data.mapper.domain.ops.SourceOp

case class FileSource(inputPath: String,
                      name: String,
                      format: IOFormat = DELTA,
                      useStringPrimitives: Boolean = false,
                      ops: Seq[SourceOp] = Seq.empty
                      )
  extends Source

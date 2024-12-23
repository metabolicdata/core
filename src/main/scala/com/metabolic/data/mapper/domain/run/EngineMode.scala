package com.metabolic.data.mapper.domain.run

object EngineMode extends Enumeration {
  type EngineMode = Value

  val Batch = Value("batch")
  val Stream = Value("stream")

}

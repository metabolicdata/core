package com.metabolic.data.mapper.domain.io

object EngineMode extends Enumeration {
  type EngineMode = Value

  val Batch = Value("batch")
  val Stream = Value("stream")

}

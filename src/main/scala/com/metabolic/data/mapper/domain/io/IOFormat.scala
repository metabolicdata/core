package com.metabolic.data.mapper.domain.io

object IOFormat extends Enumeration {
  type IOFormat = Value

  val TABLE = Value("table")
  val CSV = Value("csv")
  val PARQUET = Value("parquet")
  val JSON = Value("json")
  val DELTA =  Value("delta")
  val DELTA_PARTITION =  Value("delta_partition")
  val KAFKA =  Value("kafka")
}

package com.metabolic.data.mapper.domain.io

object WriteMode extends Enumeration {
  type WriteMode = Value

  val Append = Value("append")
  val Overwrite = Value("replace")
  val Upsert = Value("upsert")
  val Update = Value("update")
  val Delete = Value("delete")

}
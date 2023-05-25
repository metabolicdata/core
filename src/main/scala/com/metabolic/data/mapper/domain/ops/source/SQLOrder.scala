package com.metabolic.data.mapper.domain.ops.source

object SQLOrder extends Enumeration {
  type SQLOrder = Value

  val Ascending = Value("asc")
  val Descending = Value("desc")

}
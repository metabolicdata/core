package com.metabolic.data.core.services.spark.filter

object DataLakeDepth extends Enumeration {

  val YEAR, MONTH, DAY, HOUR, MINUTE = Value
  type DataLakeDepth = Value

}
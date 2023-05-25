package com.metabolic.data.core.services.spark.filter

import org.apache.spark.sql.DataFrame

trait DataframeFilter {

  def statement: String

  def filter(): DataFrame => DataFrame = { df =>

    df.filter(statement)

  }

}

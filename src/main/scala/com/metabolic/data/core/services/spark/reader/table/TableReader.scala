package com.metabolic.data.core.services.spark.reader.table

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class TableReader(fqn : String) extends DataframeUnifiedReader {

  override val input_identifier: String = fqn

  override def readBatch(spark: SparkSession): DataFrame = {
    spark.read
      .table(input_identifier)
  }

  override def readStream(spark: SparkSession): DataFrame = {
    spark.readStream
      .table(input_identifier)
  }

}

object TableReader {
  def apply(fqn: String) = new TableReader(fqn)
}
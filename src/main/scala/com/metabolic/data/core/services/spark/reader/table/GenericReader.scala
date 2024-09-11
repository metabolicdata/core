package com.metabolic.data.core.services.spark.reader.table

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenericReader(fqn: String) extends DataframeUnifiedReader {

  override val input_identifier: String = fqn

  override def readBatch(spark: SparkSession): DataFrame = {
    spark.table(input_identifier)
  }

  override def readStream(spark: SparkSession): DataFrame = {
    spark.readStream.table(input_identifier)
  }

}

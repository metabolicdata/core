package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class JSONReader(val input_identifier: String, useStringPrimitives: Boolean) extends DataframeUnifiedReader {

  override def readBatch(spark: SparkSession): DataFrame = {
    spark.read
      .option("mergeSchema", true)
      .option("primitivesAsString", useStringPrimitives)
      .json(input_identifier)
  }

  override def readStream(spark: SparkSession): DataFrame = {

    spark
      .readStream
      .option("primitivesAsString", useStringPrimitives)
      .json(input_identifier)

  }


}

object JSONReader {
  def apply(input_identifier: String) = new JSONReader(input_identifier, false)
  def apply(input_identifier: String, useStringPrimitives: Boolean) = new JSONReader(input_identifier, useStringPrimitives)
}

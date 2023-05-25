package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParquetReader(val input_identifier: String) extends DataframeUnifiedReader {

  def readBatch(spark: SparkSession): DataFrame = {
    spark.read
      .option("mergeSchema", "true")
      .parquet(input_identifier)
  }

  def readStream(spark: SparkSession): DataFrame = {
    spark.readStream
      .parquet(input_identifier)
  }

}

object ParquetReader {
  def apply(input_identifier: String) = new ParquetReader(input_identifier)
}
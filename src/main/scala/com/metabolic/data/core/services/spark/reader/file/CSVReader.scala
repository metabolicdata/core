package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class CSVReader(val input_identifier: String) extends DataframeUnifiedReader {

  def readBatch(spark: SparkSession): DataFrame = {
    spark.read
      .option("header",true)
      .option("mergeSchema", true)
      .csv(input_identifier)
  }

  def readStream(spark: SparkSession): DataFrame = {
    spark.readStream
      .csv(input_identifier)
  }

}

object CSVReader {
  def apply(input_identifier: String) = new CSVReader(input_identifier)
}

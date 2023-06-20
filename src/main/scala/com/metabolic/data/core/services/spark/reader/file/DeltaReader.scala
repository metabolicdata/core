package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeltaReader(val input_identifier: String, val startTime: Option[String]) extends DataframeUnifiedReader {

  import io.delta.implicits._

  override def readBatch(spark: SparkSession): DataFrame = {
      spark.read
        .option("startingTimestamp", startTime.toString)
        .delta(input_identifier)
  }

  override def readStream(spark: SparkSession): DataFrame = {
      spark.readStream
        .option("startingTimestamp", startTime.toString)
        .delta(input_identifier)
  }

}

object DeltaReader {
  def apply(input_identifier: String) = new DeltaReader(input_identifier, Option.empty)
  def apply(input_identifier: String, startTime: Option[String]) = new DeltaReader(input_identifier, startTime)
}

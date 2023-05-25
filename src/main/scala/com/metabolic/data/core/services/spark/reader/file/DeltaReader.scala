package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeltaReader(val input_identifier: String) extends DataframeUnifiedReader {

  import io.delta.implicits._

  override def readBatch(spark: SparkSession): DataFrame = {

    spark.read
      .delta(input_identifier)

  }

  override def readStream(spark: SparkSession): DataFrame = {

    spark.readStream
      .delta(input_identifier)

  }

}

object DeltaReader {
  def apply(input_identifier: String) = new DeltaReader(input_identifier)
}

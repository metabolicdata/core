package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import com.metabolic.data.mapper.app.MetabolicReader.logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._

class DeltaReader(val input_identifier: String, historical: Boolean, startTimestamp: String) extends DataframeUnifiedReader {

  import io.delta.implicits._

  override def readBatch(spark: SparkSession): DataFrame = {

    spark.read
      .option("timestampAsOf", startTimestamp)
      .delta(input_identifier)

  }

  override def readStream(spark: SparkSession): DataFrame = {

    val sr = spark.readStream
    val osr = historical match {
      case true   => sr.option("startingTimestamp", "2000-01-01")
      case false  => sr.option("startingTimestamp", startTimestamp)
    }

    osr
        .delta(input_identifier)
  }

}

object DeltaReader {
  def apply(input_identifier: String) = new DeltaReader(input_identifier, false, "")
  def apply(input_identifier: String, historical: Boolean, startTimestamp: String) = new DeltaReader(input_identifier, historical, startTimestamp)

}

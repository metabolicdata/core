package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import com.metabolic.data.mapper.app.MetabolicReader.logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._

class DeltaReader(val input_identifier: String, val startTime: String) extends DataframeUnifiedReader {

  import io.delta.implicits._

  override def readBatch(spark: SparkSession): DataFrame = {
    print(s"Reading source ${input_identifier} starting from ${startTime}")

    startTime match {
      case "" => spark.read.delta(input_identifier)
      case _ => {
        spark.read
          .option("timestampAsOf", startTime)
          .delta(input_identifier)
      }
    }
  }

  override def readStream(spark: SparkSession): DataFrame = {


    print(s"Reading source ${input_identifier} starting from ${startTime}")

    spark.readStream
        .option("startingTimestamp", startTime)
        .delta(input_identifier)
  }

}

object DeltaReader {
  def apply(input_identifier: String) = new DeltaReader(input_identifier, "")
  def apply(input_identifier: String, startTime: String) = new DeltaReader(input_identifier, startTime)
}

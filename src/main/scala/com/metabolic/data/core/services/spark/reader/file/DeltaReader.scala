package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeltaReader(val input_identifier: String, startTimestamp: String) extends DataframeUnifiedReader {

  import io.delta.implicits._

  override def readBatch(spark: SparkSession): DataFrame = {


    val sr = spark.read
    val osr = startTimestamp match {
      case "" => sr
      case _ => sr.option("timestampAsOf", startTimestamp)
    }
    osr
    .delta(input_identifier)

  }

  override def readStream(spark: SparkSession): DataFrame = {

    val sr = spark.readStream
    val osr = startTimestamp match{
      case "" => sr
      case _ => sr.option("startingTimestamp", startTimestamp)
    }

    osr
      .delta(input_identifier)

  }

}

object DeltaReader {
  def apply(input_identifier: String) = new DeltaReader(input_identifier, "")
  def apply(input_identifier: String, startTimestamp: String) = new DeltaReader(input_identifier,startTimestamp)

}

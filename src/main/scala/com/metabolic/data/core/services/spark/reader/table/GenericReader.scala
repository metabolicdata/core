package com.metabolic.data.core.services.spark.reader.table

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenericReader(fqn: String) extends DataframeUnifiedReader {

  override val input_identifier: String = fqn

  override def readBatch(spark: SparkSession): DataFrame = {
    spark.table(input_identifier)
  }

  override def readStream(spark: SparkSession): DataFrame = {
    //TODO: addformat delta readstream
    //spark.readStream.table(input_identifier)

    val streamStartTimestamp = System.currentTimeMillis() - 3600000 // 1 hour ago

    //format iceberg
    spark.readStream
      .format("iceberg")
      .option("stream-from-timestamp", streamStartTimestamp.toString)
      .load(input_identifier)
  }

}

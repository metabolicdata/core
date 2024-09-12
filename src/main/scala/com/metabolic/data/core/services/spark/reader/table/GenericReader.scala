package com.metabolic.data.core.services.spark.reader.table

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenericReader(fqn: String) extends DataframeUnifiedReader {

  override val input_identifier: String = fqn

  override def readBatch(spark: SparkSession): DataFrame = {
    //Generic for Delta Lake and Iceberg tables using fqn
    spark.table(input_identifier)
  }

  override def readStream(spark: SparkSession): DataFrame = {
    //TODO: addformat delta readstream
    //format Delta
    //spark.readStream.format("delta").table("campaign_all_delta_stream")

    //format Iceberg
    spark.readStream
      .format("iceberg")
      .option("stream-from-timestamp", (System.currentTimeMillis() - 3600000).toString)
      .load(input_identifier)
  }

}

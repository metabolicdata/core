package com.metabolic.data.core.services.spark.reader.stream

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class KinesisReader(region: String, stream: String,
                    awsAccessKeyId: String, awsSecretKey: String)
  extends DataframeUnifiedReader {

  override val input_identifier: String = stream

  override def readStream(spark: SparkSession): DataFrame = {

    spark
      .readStream
      .format("kinesis")
      .option("streamName", stream)
      .option("region", region)
      .option("initialPosition", "TRIM_HORIZON")
      .option("awsAccessKey", awsAccessKeyId)
      .option("awsSecretKey", awsSecretKey)
      .load()

  }

  override def readBatch(spark: SparkSession): DataFrame = {
    spark
      .read
      .format("kinesis")
      .option("streamName", stream)
      .option("region", region)
      .option("initialPosition", "TRIM_HORIZON")
      .option("awsAccessKey", awsAccessKeyId)
      .option("awsSecretKey", awsSecretKey)
      .load()

  }
}

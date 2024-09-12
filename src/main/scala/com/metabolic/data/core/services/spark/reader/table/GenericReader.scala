package com.metabolic.data.core.services.spark.reader.table

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.delta.implicits.stringEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenericReader(fqn: String) extends DataframeUnifiedReader with Logging{

  override val input_identifier: String = fqn

  private def getTableProvider(spark: SparkSession): String = {
    spark.sql(s"DESCRIBE FORMATTED $input_identifier")
      .filter(col("col_name").contains("Provider"))
      .select("data_type")
      .as[String]
      .first()
  }

  override def readBatch(spark: SparkSession): DataFrame = {
    //Generic for Delta Lake and Iceberg tables using fqn
    spark.table(input_identifier)
  }

  override def readStream(spark: SparkSession): DataFrame = {

    val provider = getTableProvider(spark)
    provider match {
      case "iceberg" =>
        logger.info(s"Reading Iceberg Table source ${input_identifier}")
        spark.readStream
          .format("iceberg")
          .option("stream-from-timestamp", (System.currentTimeMillis() - 3600000).toString)
          .load(input_identifier)

      case "delta" =>
        logger.info(s"Reading Delta Table source $input_identifier")
        spark.readStream
          .format("delta")
          .table(input_identifier)

      case unknown =>
        logger.warn(s"Table source $provider not supported for table $input_identifier")
        spark.emptyDataFrame
    }
  }

}

package com.metabolic.data.core.services.spark.reader.table

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.delta.implicits.stringEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenericReader(fqn: String) extends DataframeUnifiedReader with Logging{

  override val input_identifier: String = fqn

  override def readBatch(spark: SparkSession): DataFrame = {
    //Generic for Delta Lake and Iceberg tables using fqn
    spark.table(input_identifier)
  }

  override def readStream(spark: SparkSession): DataFrame = {
    //Generic for Delta Lake and Iceberg tables using fqn
    spark.readStream.table(input_identifier)
  }

}

package com.metabolic.data.core.services.spark.reader.table

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

class GenericReader(fqn: String) extends DataframeUnifiedReader with Logging{

  override val input_identifier: String = fqn

  override def readBatch(spark: SparkSession): DataFrame = {
    //Generic for Delta Lake and Iceberg tables using fqn with error handling
    try {
      spark.table(input_identifier)
    } catch {
      case e: AnalysisException if e.getMessage.contains("Unable to fetch table") =>
        throw new IllegalArgumentException(s"Table '$input_identifier' is not properly defined: ${e.getMessage}")
      case e: AnalysisException =>
        throw new RuntimeException(s"Failed to read table '$input_identifier': ${e.getMessage}", e)
      case e: Exception =>
        throw new RuntimeException(s"Unexpected error when reading table '$input_identifier': ${e.getMessage}", e)
    }
  }

  override def readStream(spark: SparkSession): DataFrame = {
    //Generic for Delta Lake and Iceberg tables using fqn with error handling
    try {
      spark.readStream.table(input_identifier)
    }
    catch {
      case e: AnalysisException if e.getMessage.contains("Unable to fetch table") =>
        throw new IllegalArgumentException(s"Table '$input_identifier' is not properly defined: ${e.getMessage}")
      case e: AnalysisException =>
        throw new RuntimeException(s"Failed to read table '$input_identifier': ${e.getMessage}", e)
      case e: Exception =>
        throw new RuntimeException(s"Unexpected error when reading table '$input_identifier': ${e.getMessage}", e)
    }
  }

}

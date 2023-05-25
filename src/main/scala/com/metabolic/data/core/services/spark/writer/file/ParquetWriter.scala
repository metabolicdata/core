package com.metabolic.data.core.services.spark.writer.file

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode}

class ParquetWriter(outputPath: String, val saveMode: SaveMode, val checkpointLocation: String)
  extends DataframeUnifiedWriter {

  override val output_identifier: String = outputPath

  override def writeBatch(df: DataFrame): Unit = {

    df
      .write
      .mode(saveMode)
      .parquet(outputPath)

  }

  override def writeStream(df: DataFrame): StreamingQuery = {

    df
      .writeStream
      .format("parquet")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointLocation)
      .start()

  }
}




package com.metabolic.data.core.services.spark.writer.file

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode}

class JSONWriter(outputPath: String, val saveMode: SaveMode, val checkpointLocation: String)
  extends DataframeUnifiedWriter {

  override val output_identifier: String = outputPath

  override def writeBatch(df: DataFrame): Unit = {

    df
      .write
      .mode(saveMode)
      .json(outputPath)

  }

  override def writeStream(df: DataFrame): StreamingQuery = {

    df
      .writeStream
      .format("json")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointLocation)
      .start()

  }
}

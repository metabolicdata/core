package com.metabolic.data.core.services.spark.writer.file

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode

class CSVWriter(outputPath: String, val writeMode: WriteMode, val checkpointLocation: String)
  extends DataframeUnifiedWriter {

  override val output_identifier: String = outputPath

  override def writeBatch(df: DataFrame): Unit = {

    writeMode match {
      case WriteMode.Append => df
        .write
        .mode(SaveMode.Append)
        .option("header", true)
        .csv(outputPath)
      case WriteMode.Overwrite => df
        .write
        .mode(SaveMode.Overwrite)
        .option("header", true)
        .csv(outputPath)
    }

  }

  override def writeStream(df: DataFrame): StreamingQuery = {

    df
      .writeStream
      .format("csv")
      .option("path", outputPath)
      .option("header", true)
      .option("checkpointLocation", checkpointLocation)
      .start()

  }

}

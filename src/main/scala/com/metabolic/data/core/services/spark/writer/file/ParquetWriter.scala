package com.metabolic.data.core.services.spark.writer.file

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode}

class ParquetWriter(outputPath: String, val writeMode: WriteMode, val checkpointLocation: String)
  extends DataframeUnifiedWriter {

  override val output_identifier: String = outputPath

  override def writeBatch(df: DataFrame): Unit = {

    writeMode match {
      case WriteMode.Append => df
        .write
        .mode(SaveMode.Append)
        .parquet(outputPath)
      case WriteMode.Overwrite => df
        .write
        .mode(SaveMode.Overwrite)
        .parquet(outputPath)
    }

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




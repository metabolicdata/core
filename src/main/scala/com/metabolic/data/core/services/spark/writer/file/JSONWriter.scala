package com.metabolic.data.core.services.spark.writer.file

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode}

class JSONWriter(outputPath: String, val writeMode: WriteMode, val checkpointLocation: String)
  extends DataframeUnifiedWriter {

  override val output_identifier: String = outputPath

  override def writeBatch(df: DataFrame): Unit = {

    writeMode match {
      case WriteMode.Append => df
        .write
        .mode(SaveMode.Append)
        .json(outputPath)
      case WriteMode.Overwrite => df
        .write
        .mode(SaveMode.Overwrite)
        .json(outputPath)
    }

  }

  override def writeStream(df: DataFrame): Seq[StreamingQuery] = {

    val query = df
      .writeStream
      .format("json")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointLocation)
      .start()

    Seq(query)
  }
}

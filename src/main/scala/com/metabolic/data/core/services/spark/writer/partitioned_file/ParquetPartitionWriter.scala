package com.metabolic.data.core.services.spark.writer.partitioned_file

import com.metabolic.data.core.services.spark.writer.DataframePartitionWriter
import com.metabolic.data.core.services.spark.writer.file.ParquetWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode}

class ParquetPartitionWriter(val partitionColumnNames: Seq[String],
                             val outputPath: String,
                             writeMode: WriteMode, baseCheckpointLocation: String)
  extends ParquetWriter(outputPath, writeMode, baseCheckpointLocation)
    with DataframePartitionWriter {

  override val output_identifier: String = outputPath

  override def writeBatch(df: DataFrame): Unit = {

    if (partitionColumnNames.size > 0) {
      writeMode match {
        case WriteMode.Append => df
          .write
          .partitionBy(partitionColumnNames: _*)
          .mode(SaveMode.Append)
          .parquet(outputPath)
        case WriteMode.Overwrite => df
          .write
          .partitionBy(partitionColumnNames: _*)
          .mode(SaveMode.Overwrite)
          .parquet(outputPath)
      }
    } else {
      super.writeBatch(df)
    }
  }

  override def writeStream(df: DataFrame): StreamingQuery = {

    if(partitionColumnNames.size > 0) {

      df
        .writeStream
        .partitionBy(partitionColumnNames: _*)
        .format("parquet")
        .option("path", outputPath)
        .start()

    } else {
      super.writeStream(df)
    }

  }

}




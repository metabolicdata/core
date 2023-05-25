package com.metabolic.data.core.services.spark.writer.partitioned_file

import com.metabolic.data.core.services.spark.writer.DataframePartitionWriter
import com.metabolic.data.core.services.spark.writer.file.ParquetWriter
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode}

class ParquetPartitionWriter(val partitionColumnNames: Seq[String],
                             val outputPath: String,
                             saveMode: SaveMode, baseCheckpointLocation: String)
  extends ParquetWriter(outputPath, saveMode, baseCheckpointLocation)
    with DataframePartitionWriter {

  override val output_identifier: String = outputPath

  override def writeBatch(df: DataFrame): Unit = {


    if(partitionColumnNames.size > 0) {
      df
        .write
        .partitionBy(partitionColumnNames: _*)
        .mode(saveMode)
        .parquet(outputPath)

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




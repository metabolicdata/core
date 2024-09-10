package com.metabolic.data.core.services.spark.writer.file

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}

class IcebergWriter(
                     val fqn: String,
                     val writeMode: WriteMode,
                     val checkpointLocation: String)
                   (implicit  val spark: SparkSession)
  extends DataframeUnifiedWriter {

  override val output_identifier: String = fqn

  override def writeBatch(df: DataFrame): Unit = {

    writeMode match {
      case WriteMode.Append => df
          .write
          .format("iceberg")
          .mode("append")
          .saveAsTable(output_identifier)

      case WriteMode.Overwrite => df
        .write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(output_identifier)

      case WriteMode.Upsert =>
        throw new NotImplementedError("Batch Upsert is not supported by Iceberg yet")
    }

  }

  override def writeStream(df: DataFrame): StreamingQuery = {

    writeMode match {
      case WriteMode.Append =>
        df
          .writeStream
          .format("iceberg")
          .outputMode("append")
          .option("checkpointLocation", checkpointLocation)
          .toTable(output_identifier)

      case WriteMode.Overwrite =>
        throw new NotImplementedError("Streaming Overwrite is not supported by Iceberg yet")

      case WriteMode.Upsert =>
        throw new NotImplementedError("Streaming Upsert is not supported by Iceberg yet")
    }
  }

  override def postHook(df: DataFrame, query: Seq[StreamingQuery]): Unit = {

    if (query.isEmpty) {
      spark.sql(s"CALL local.system.rewrite_data_files('$output_identifier')")
    }
  }

}

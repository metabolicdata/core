package com.metabolic.data.core.services.spark.writer.file

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.concurrent.TimeUnit

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
        throw new NotImplementedError("Batch Upsert is not supported in Iceberg yet")

      case WriteMode.Delete =>
        throw new NotImplementedError("Delete is not supported in Iceberg yet")

      case WriteMode.Update =>
        throw new NotImplementedError("Update is not supported in Iceberg yet")

    }
  }

  override def writeStream(df: DataFrame): StreamingQuery = {

    writeMode match {
      case WriteMode.Append =>
        df
          .writeStream
          .format("iceberg")
          .outputMode("append")
          .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
          .option("checkpointLocation", checkpointLocation)
          .toTable(output_identifier)

      case WriteMode.Complete =>
        df
          .writeStream
          .format("iceberg")
          .outputMode("complete")
          .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
          .option("checkpointLocation", checkpointLocation)
          .toTable(output_identifier)

    }
  }

//TODO: do we need to do any specific post write operations in Iceberg?
//
//  override def postHook(df: DataFrame, query: Seq[StreamingQuery]): Unit = {
//
//    if (query.isEmpty) {
//      spark.sql(s"CALL local.system.rewrite_data_files('$output_identifier')")
//    }
//  }

}

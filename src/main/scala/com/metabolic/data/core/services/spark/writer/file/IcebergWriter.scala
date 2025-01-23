package com.metabolic.data.core.services.spark.writer.file

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

import java.util.concurrent.TimeUnit

class IcebergWriter(
                     val fqn: String,
                     val writeMode: WriteMode,
                     val idColumnName: Option[String],
                     val checkpointLocation: String)
                   (implicit  val spark: SparkSession)
  extends DataframeUnifiedWriter {

  override val output_identifier: String = fqn

  private val idColumnNameIceberg: String = idColumnName.getOrElse("")

  override def writeBatch(df: DataFrame): Unit = {

    writeMode match {
      case WriteMode.Append =>
        try {
          df.writeTo(output_identifier).using("iceberg").create()
        }catch {
          case e: AnalysisException =>
            logger.warn("Create table failed: " + e)
            df.writeTo(output_identifier).append()
        }

      case WriteMode.Overwrite =>
        try {
          df.writeTo(output_identifier).using("iceberg").create()
        }catch {
          case e: AnalysisException =>
            logger.warn("Create table failed: " + e)
            df.writeTo(output_identifier).using("iceberg").replace()
        }

      case WriteMode.Upsert =>
        try {
          df.writeTo(output_identifier).using("iceberg").create()
        }catch {
          case e: AnalysisException =>
            logger.warn("Create table failed: " + e)
            df.createOrReplaceTempView("merge_data_view")
            try {
              val keyColumns = idColumnNameIceberg.split(",")
              val onCondition = if (keyColumns.length == 1) {
                s"target.${keyColumns.head} = source.${keyColumns.head}"
              } else {
                keyColumns.map(column => s"target.$column = source.$column").mkString(" AND ")
              }
              // Merge DataFrame implementation is only available on spark > 4.0.0
              val merge_query = {
                f"""
                MERGE INTO $output_identifier AS target
                USING merge_data_view AS source
                ON $onCondition
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
              }
              spark.sql(merge_query)
            } catch {
              case e: Exception =>
                logger.error(s"Error while performing upsert on $output_identifier: ${e.getMessage}")
                throw e
            }
        }

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

package com.metabolic.data.core.services.spark.writer.file

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.concurrent.TimeUnit

class IcebergWriter(
                     val fqn: String,
                     val writeMode: WriteMode,
                     val idColumnNames: Option[Seq[String]],
                     val checkpointLocation: String,
                     val partitionColumnNames: Option[Seq[String]] = None)
                   (implicit val spark: SparkSession)
  extends DataframeUnifiedWriter {

  override val output_identifier: String = fqn

  private val idColumnNamesIceberg: String = idColumnNames.getOrElse(Seq.empty).toList.mkString(",")

  private def createTableIfNotExists(df: DataFrame): Unit = {
    val partitionClause = partitionColumnNames match {
      case Some(cols) if cols.nonEmpty => s"PARTITIONED BY (${cols.mkString(", ")})"
      case _ => ""
    }

    val schemaDefinition = df.schema.fields.map(f => s"${f.name} ${f.dataType.sql}").mkString(", ")

    val createTableStmt =
      s"""
         |CREATE TABLE IF NOT EXISTS $output_identifier (
         |  $schemaDefinition
         |)
         |USING iceberg
         |$partitionClause
         |""".stripMargin

    spark.sql(createTableStmt)
  }

  override def writeBatch(df: DataFrame): Unit = {

    createTableIfNotExists(df)

    writeMode match {
      case WriteMode.Append =>
        val supportEvolution =
          s"""
             |ALTER TABLE $output_identifier SET TBLPROPERTIES (
             |  'write.spark.accept-any-schema'='true'
             |)
          """.stripMargin

        spark.sql(supportEvolution)

        df.writeTo(output_identifier).option("mergeSchema", "true").append()

      case WriteMode.Overwrite =>
        partitionColumnNames match {
          case Some(cols) if cols.nonEmpty =>
            val partitionColumns: List[String] = List("year", "month", "day")
            val partitionCols: List[org.apache.spark.sql.Column] = partitionColumns.map(col)

            df.writeTo(output_identifier).using("iceberg").partitionedBy(partitionCols.head, partitionCols.tail: _*).replace()
          case _ => df.writeTo(output_identifier).using("iceberg").replace()
        }

      case WriteMode.Upsert =>
        // Step 1: Alter table to include new columns from DataFrame
        try {
          val disableEvolution =
            s"""
               |ALTER TABLE $output_identifier SET TBLPROPERTIES (
               |  'write.spark.accept-any-schema'='false'
               |)
          """.stripMargin

          spark.sql(disableEvolution)

          val tableSchema = spark.table(output_identifier).schema
          val incomingSchema = df.schema

          val newColumns = incomingSchema.filterNot(f => tableSchema.fieldNames.contains(f.name))

          newColumns.foreach { field =>
            val columnName = field.name
            val dataType = field.dataType.catalogString
            val alterSQL = s"ALTER TABLE $output_identifier ADD COLUMN $columnName $dataType"
            spark.sql(alterSQL)
            logger.info(s"Added new column to $output_identifier: $columnName $dataType")
          }
        } catch {
          case e: Exception =>
            logger.error(s"Error during upsert schema evolution on $output_identifier: ${e.getMessage}")
            throw e
        }

        // Step 2: Merge
        df.createOrReplaceTempView("merge_data_view")
        try {
          val keyColumns = idColumnNamesIceberg.replaceAll("\"", "").split(",")
          val onCondition = if (keyColumns.length == 1) {
            s"target.${keyColumns.head} = source.${keyColumns.head}"
          } else {
            keyColumns.map(column => s"target.$column = source.$column").mkString(" AND ")
          }

          val merge_query =
            s"""
               |MERGE INTO $output_identifier AS target
               |USING merge_data_view AS source
               |ON $onCondition
               |WHEN MATCHED THEN UPDATE SET *
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin

          spark.sql(merge_query)

        } catch {
          case e: Exception =>
            logger.error(s"Error while performing upsert on $output_identifier: ${e.getMessage}")
            throw e
        }

      case WriteMode.Delete =>
        try {
          spark.sql(s"DROP TABLE IF EXISTS $output_identifier PURGE")
          logger.info(s"Iceberg table $output_identifier has been dropped with PURGE.")
        } catch {
          case e: Exception =>
            logger.error(s"Error while dropping table $output_identifier: ${e.getMessage}")
            throw e
        }

      case WriteMode.Update =>
        throw new NotImplementedError("Update is not supported in Iceberg yet")

    }
  }

  override def writeStream(df: DataFrame): StreamingQuery = {

    createTableIfNotExists(df)

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

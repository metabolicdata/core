package com.metabolic.data.core.services.spark.writer.partitioned_file

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.glue.{AthenaCatalogueService, GlueCatalogService}
import com.metabolic.data.core.services.spark.writer.DataframePartitionWriter
import com.metabolic.data.core.services.spark.writer.file.DeltaWriter
import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import io.delta.implicits.DeltaDataFrameWriter
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

import scala.collection.Seq
import scala.reflect.io.File

class DeltaPartitionWriter(val partitionColumnNames: Seq[String],
                           outputPath: String,
                           writeMode: WriteMode,
                           dateColumnName: Option[String],
                           idColumnName: Option[String],
                           dbName: String,
                           override val checkpointLocation: String,
                           namespaces: Seq[String])(implicit regions: Regions, spark: SparkSession)

  extends DeltaWriter(outputPath, writeMode, dateColumnName, idColumnName, checkpointLocation, dbName, namespaces)
    with DataframePartitionWriter {

  override val output_identifier: String = outputPath

  override def writeBatch(df: DataFrame): Unit = {
    if (partitionColumnNames.size > 0) {
      writeMode match {
        case WriteMode.Append => df
          .write
          .partitionBy(partitionColumnNames: _*)
          .mode(SaveMode.Append)
          .option("mergeSchema", "true")
          .delta(outputPath)
        case WriteMode.Overwrite => df
          .write
          .partitionBy(partitionColumnNames: _*)
          .mode(SaveMode.Overwrite)
          .option("overwriteSchema", "true")
          .delta(outputPath)
        case WriteMode.Upsert => upsertToDelta(df)
      }
    } else {
      super.writeBatch(df)
    }
  }

  override def writeStream(df: DataFrame): StreamingQuery = {
    if (partitionColumnNames.size > 0) {
      writeMode match {
        case WriteMode.Append => df
          .writeStream
          .partitionBy(partitionColumnNames: _*)
          .outputMode("append")
          .option("mergeSchema", "true")
          .option("checkpointLocation", checkpointLocation)
          .start(output_identifier)
        case WriteMode.Overwrite => df
          .writeStream
          .partitionBy(partitionColumnNames: _*)
          .outputMode("complete")
          .option("overwriteSchema", "true")
          .option("checkpointLocation", checkpointLocation)
          .start(output_identifier)
        case WriteMode.Upsert => df
          .writeStream
          .format("delta")
          .partitionBy(partitionColumnNames: _*)
          .foreachBatch(upsertToDelta _)
          .outputMode("update")
          .option("mergeSchema", "true")
          .start(output_identifier)
      }
    }
    else {
      super.writeStream(df)
    }
  }

  override def preHook(df: DataFrame): DataFrame = {

    if (partitionColumnNames.size > 0) {
      val tableName: String = ConfigUtilsService.getTablePrefix(namespaces, output_identifier)+ConfigUtilsService.getTableNameFileSink(output_identifier)

      if (!DeltaTable.isDeltaTable(outputPath)) {
        if (!File(outputPath).exists) {
          //In this way a table is created which can be read but the schema is not visible.
          /*
          //Check if the database has location
          new GlueCatalogService()
            .checkDatabase(dbName, ConfigUtilsService.getDataBaseName(outputPath))
          //Create the delta table
          val deltaTable = DeltaTable.createIfNotExists()
            .tableName(dbName + "." + tableName)
            .location(output_identifier)
            .addColumns(df.schema)
            .partitionedBy(partitionColumnNames: _*)
            .execute()
          deltaTable.toDF.write.format("delta").mode(SaveMode.Append).save(output_identifier)
          //Create table in Athena (to see the columns)
          new AthenaCatalogueService()
            .createDeltaTable(dbName, tableName, output_identifier, true)*/

          //Redo if the other way works for us
          // create an empty RDD with original schema
          val emptyRDD = spark.sparkContext.emptyRDD[Row]
          val emptyDF = spark.createDataFrame(emptyRDD, df.schema)
          emptyDF
            .write
            .format("delta")
            .mode(SaveMode.Append)
            .partitionBy(partitionColumnNames: _*)
            .save(output_identifier)
          //Create table in Athena
          new AthenaCatalogueService()
            .createDeltaTable(dbName, tableName, output_identifier)

        } else {
          //Convert to delta if parquet
          DeltaTable.convertToDelta(spark, s"parquet.`$outputPath`")
        }
      }
      df
      
    } else {
      super.preHook(df)
    }

  }

  override def postHook(df: DataFrame, query: Option[StreamingQuery]): Boolean = {
    //Not for current version
    //deltaTable.optimize().executeCompaction()
    query.flatMap(stream => Option.apply(stream.awaitTermination()))

    true
  }
}




package com.metabolic.data.core.services.spark.writer.file

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.glue.AthenaCatalogueService
import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import io.delta.tables._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.reflect.io.File

class DeltaWriter(val outputPath: String, val writeMode: WriteMode,
                  val dateColumnName: Option[String], val idColumnName: Option[String],
                  val checkpointLocation: String,
                  val dbName: String, val namespaces: Seq[String], val retention: Double = 168d,
                  val optimizeEvery: String = "1 hour") (implicit val region: Regions, implicit  val spark: SparkSession)

  extends DataframeUnifiedWriter {

    import io.delta.implicits._

    override val output_identifier: String = outputPath

    private val dateColumnNameDelta: String = dateColumnName.getOrElse("")
    private val idColumnNameDelta: String = idColumnName.getOrElse("")

    protected def compactAndVacuum(): Unit = {
      val deltaTable = DeltaTable.forPath(outputPath)
      deltaTable.optimize().executeCompaction()
      deltaTable.vacuum(retention)
    }

    def upsertToDelta(df: DataFrame, batchId: Long = 1L): Unit = { //batchId is used for streaming
      val mergeStatement = dateColumnNameDelta match {
        case "" =>
          s"output.${idColumnNameDelta} = updates.${idColumnNameDelta}"
        case _ =>
          s"output.${idColumnNameDelta} = updates.${idColumnNameDelta} AND" +
            s" output.${dateColumnNameDelta} = updates.${dateColumnNameDelta}"
      }
      DeltaTable.forPath(outputPath).as("output")
        .merge(
          df.as("updates"), mergeStatement
        )
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }

  def deleteToDelta(df: DataFrame, batchId: Long = 1L): Unit = { //batchId is used for streaming
    val deleteStatement = dateColumnNameDelta match {
      case "" =>
        s"output.${idColumnNameDelta} = deletes.${idColumnNameDelta}"
      case _ =>
        s"output.${idColumnNameDelta} = deletes.${idColumnNameDelta} AND" +
          s" output.${dateColumnNameDelta} = deletes.${dateColumnNameDelta}"
    }
    DeltaTable.forPath(outputPath).as("output")
      .merge(
        df.as("deletes"), deleteStatement
      )
      .whenMatched().delete()
      .execute()
  }

  def optimizeDeltaInStreaming(df: DataFrame, batchId: Long = 1L): Unit = {
      compactAndVacuum
  }

  override def writeBatch(df: DataFrame): Unit = {

      writeMode match {
        case WriteMode.Append => df
          .write
          .mode(SaveMode.Append)
          .option("mergeSchema", "true")
          .delta(outputPath)
        case WriteMode.Overwrite => df
          .write
          .mode(SaveMode.Overwrite)
          .option("overwriteSchema", "true")
          .delta(outputPath)
        case WriteMode.Upsert =>
          //Append empty to force schema update then upsert
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df.schema)
            .write
            .mode(SaveMode.Append)
            .option("mergeSchema", "true")
            .delta(outputPath)

          upsertToDelta(df)
        case WriteMode.Delete =>
          deleteToDelta(df)
        case WriteMode.Update =>
      }

    }

    override def writeStream(df: DataFrame): Seq[StreamingQuery] = {

      val data_query = writeMode match {
        case WriteMode.Append => df
          .writeStream
          .outputMode("append")
          .option("mergeSchema", "true")
          .option("checkpointLocation", checkpointLocation)
          .delta(output_identifier)

        case WriteMode.Overwrite => df
          .writeStream
          .outputMode("complete")
          .option("overwriteSchema", "true")
          .option("checkpointLocation", checkpointLocation)
          .delta(output_identifier)

        case WriteMode.Upsert => df
          .writeStream
          .outputMode("append")
          .option("checkpointLocation", checkpointLocation)
          .foreachBatch(upsertToDelta _)
          .start
        }

      val opt_query = df
        .writeStream
        .trigger(Trigger.ProcessingTime(optimizeEvery))
        .foreachBatch(optimizeDeltaInStreaming _)
        .start()

      Seq(data_query,opt_query)
    }


  override def preHook(df: DataFrame): DataFrame = {

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
          .save(output_identifier)

      } else {
        //Convert to delta if parquet
        DeltaTable.convertToDelta(spark, s"parquet.`$outputPath`")
      }
    }
    df
  }

  override def postHook(df: DataFrame, query: Seq[StreamingQuery]): Unit = {

    if (query.isEmpty) {
      compactAndVacuum
    }

  }

}

package com.metabolic.data.core.services.spark.writer.file

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import io.delta.tables._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.reflect.io.File

class DeltaWriter(val outputPath: String, val writeMode: WriteMode,
                  val dateColumnName: Option[String], val idColumnName: Option[String],
                  val checkpointLocation: String,
                  val dbName: String, val namespaces: Seq[String],
                  val optimizeOption : Option[Boolean] = None, val optimizeEveryOption: Option[Int] = None,
                  val retention: Double = 168d) (implicit val region: Regions, implicit  val spark: SparkSession)

  extends DataframeUnifiedWriter {

    import io.delta.implicits._

    override val output_identifier: String = outputPath

    private val dateColumnNameDelta: String = dateColumnName.getOrElse("")
    private val idColumnNameDelta: String = idColumnName.getOrElse("")

    private val optimizeEvery = optimizeEveryOption.getOrElse(10)
    private val optimize = optimizeOption.getOrElse(true)

    protected def compactAndVacuum(): Unit = {
      logger.info(s"Compacting Delta table $outputPath")
      val deltaTable = DeltaTable.forPath(outputPath)
      deltaTable.optimize().executeCompaction()
      logger.info(s"Vacumming Delta table $outputPath with retention $retention")
      deltaTable.vacuum(retention)
    }

    def appendToDelta(df: DataFrame, batchId: Long = 1L): Unit = {

      DeltaTable.forPath(outputPath).as("output")

        df.write
          .mode(SaveMode.Append)
          .option("mergeSchema", "true")
          .option("txnVersion", batchId).option("txnAppId", output_identifier)
          .delta(output_identifier)

      if (batchId % optimizeEvery == 0) {
        compactAndVacuum
      }

    }

  def replaceToDelta(df: DataFrame, batchId: Long = 1L): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("overwriteSchema", "true")
      .option("txnVersion", batchId).option("txnAppId", output_identifier)
      .delta(output_identifier)

    if (batchId % optimizeEvery == 0) {
      compactAndVacuum
    }

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

      if (batchId % optimizeEvery == 0) {
        compactAndVacuum
      }
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

    if (batchId % optimizeEvery == 0) {
      compactAndVacuum
    }
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

    override def writeStream(df: DataFrame): StreamingQuery = {

      val data_query = writeMode match {
        case WriteMode.Append => df

          val baseQuery = df
            .writeStream
            .outputMode("append")
            .option("mergeSchema", "true")
            .option("checkpointLocation", checkpointLocation)

          if (optimize) {
            baseQuery.foreachBatch(appendToDelta _).start
          } else {
            baseQuery.start
          }

        case WriteMode.Overwrite =>

          DeltaTable.forPath(outputPath).delete()

          df
            .writeStream
            .outputMode("append")
            .option("overwriteSchema", "true")
            .option("checkpointLocation", checkpointLocation)
            .foreachBatch(appendToDelta _)
            .start

        case WriteMode.Upsert => df
          .writeStream
          .outputMode("append")
          .option("checkpointLocation", checkpointLocation)
          .foreachBatch(upsertToDelta _)
          .start
        }

      data_query
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

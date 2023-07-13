package com.metabolic.data.core.services.spark.writer.file

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.glue.{AthenaCatalogueService, GlueCatalogService}
import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.core.services.util.ConfigUtilsService
import io.delta.tables._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.reflect.io.File

class DeltaWriter(val outputPath: String, val saveMode: SaveMode,
                  val dateColumnName: Option[String], val idColumnName: Option[String],
                  val upsert: Boolean, val checkpointLocation: String,
                  val dbName: String, namespaces: Seq[String]) (implicit val region: Regions, implicit  val spark: SparkSession)

  extends DataframeUnifiedWriter {

    import io.delta.implicits._

    override val output_identifier: String = outputPath

    private val dateColumnNameDelta: String = dateColumnName.getOrElse("")
    private val idColumnNameDelta: String = idColumnName.getOrElse("")
    var deltaTable: DeltaTable = null


    override def writeBatch(df: DataFrame): Unit = {

      upsert match {
        case false => {
          df
            .write
            .mode(saveMode)
            .option("overwriteSchema", "true")
            .option("mergeSchema", "true")
            .delta(outputPath)
        }
        case true => upsertToDelta(df)

      }

    }

    override def writeStream(df: DataFrame): StreamingQuery = {

      upsert match {
        case false => {
          saveMode match {
            case SaveMode.Append => {
              df.writeStream
                .format("delta")
                .outputMode("append")
                .option("mergeSchema", "true")
                .option("checkpointLocation", checkpointLocation)
                .start(output_identifier)
            }
            case SaveMode.Overwrite => {
              df.writeStream
                .format("delta")
                .outputMode("complete")
                .option("mergeSchema", "true")
                .option("checkpointLocation", checkpointLocation)
                .start(output_identifier)
            }
          }
        }
        case true => {
          df.writeStream
            .format("delta")
            .foreachBatch(upsertToDelta _)
            .outputMode("append")
            .option("mergeSchema", "true")
            .option("checkpointLocation", checkpointLocation)
            .start(output_identifier)
        }
      }

    }

  def upsertToDelta(df: DataFrame, batchId: Long = 1): Unit = {
    val mergeStatement = dateColumnNameDelta match {
      case "" =>
        s"output.${idColumnNameDelta} = updates.${idColumnNameDelta}"
      case _ =>
        s"output.${idColumnNameDelta} = updates.${idColumnNameDelta} AND" +
          s" output.${dateColumnNameDelta} = updates.${dateColumnNameDelta}"
    }
    deltaTable.as("output")
      .merge(
        df.as("updates"), mergeStatement
      )
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()
  }

  override def preHook(df: DataFrame): DataFrame = {

    val prefix = ConfigUtilsService.getTablePrefix(namespaces, output_identifier)
    val tableName: String = prefix + ConfigUtilsService.getTableNameFileSink(output_identifier)
    if (!DeltaTable.isDeltaTable(outputPath)) {
      if (!File(outputPath).exists) {
        // create an empty RDD with original schema
        val emptyRDD = spark.sparkContext.emptyRDD[Row]
        val emptyDF = spark.createDataFrame(emptyRDD, df.schema)
        emptyDF
          .write
          .format("delta")
          .mode(SaveMode.Append)
          .save(output_identifier)
        //Create table in Athena
        new AthenaCatalogueService()
          .createDeltaTable(dbName, tableName, output_identifier)
        //Create table in Athena separate schema
        new AthenaCatalogueService()
          .createDeltaTable(dbName + "_" + prefix.dropRight(1), tableName, output_identifier)

      } else {
        //Convert to delta if parquet
        DeltaTable.convertToDelta(spark, s"parquet.`$outputPath`")
      }
    }
    deltaTable = DeltaTable.forPath(outputPath)
    df
  }

  override def postHook(df: DataFrame, query: Option[StreamingQuery]): Boolean = {
    //Not for current version
    //deltaTable.optimize().executeCompaction()
    //deltaTable.optimize().executeZOrderBy("column")

    query.flatMap(stream => Option.apply(stream.awaitTermination()))
    true
  }

}

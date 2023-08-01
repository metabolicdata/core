package com.metabolic.data.core.services.spark.writer

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.spark.reader.file.DeltaReader
import com.metabolic.data.mapper.domain.io.EngineMode
import io.delta.implicits.{DeltaDataFrameWriter, DeltaDataStreamReader}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import scala.reflect.io.Directory
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class DeltaReaderTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  val delta_source_path = "src/test/tmp/delta/letters_5"
  val path = "src/test/tmp/delta/letters_6"
  val pathCheckpoint = "src/test/tmp/delta/letters_6_checkpoint"

  val eventA = Seq(
    Row("A", "a", 2022, 2, 5, "2022-02-05")
  )

  val someSchema = List(
    StructField("name", StringType, true),
    StructField("data", StringType, true),
    StructField("yyyy", IntegerType, true),
    StructField("mm", IntegerType, true),
    StructField("dd", IntegerType, true),
    StructField("date", StringType, true),
  )

  def write_into_source(df: DataFrame, path: String, savemode: SaveMode): Unit = {
    df
      .write
      .mode(savemode)
      .option("overwriteSchema", "true")
      .option("mergeSchema", "true")
      .delta(path)
  }

  test("Create Delta source for streaming with event A"){
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(eventA),
      StructType(someSchema)
    )

    val directoryPath = new Directory(new File(delta_source_path))
    directoryPath.deleteRecursively()

    write_into_source(inputDF, delta_source_path, SaveMode.Overwrite)
    val outputDf = DeltaReader(delta_source_path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)
    assertDataFrameNoOrderEquals(inputDF, outputDf)

  }

  test("Add event B to the source") {
    val sqlCtx = sqlContext

    val eventB = Seq(
      Row("B", "b", 2022, 2, 6, "2022-02-06")
    )

    val sourceData = Seq(
      Row("A", "a", 2022, 2, 5, "2022-02-05"),
      Row("B", "b", 2022, 2, 6, "2022-02-06")
    )

    val eventBdf = spark.createDataFrame(
      spark.sparkContext.parallelize(eventB),
      StructType(someSchema)
    )

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(sourceData),
      StructType(someSchema)
    )
    Thread.sleep(10000)
    write_into_source(eventBdf, delta_source_path, SaveMode.Append)
    val outputDf = DeltaReader(delta_source_path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)
    assertDataFrameNoOrderEquals(sourceDf, outputDf)
  }

  test("Read after event b committed time should not return event A") {
    val sqlCtx = sqlContext

    val directoryPath = new Directory(new File(path))
    directoryPath.deleteRecursively()

    val directoryCheckpoint = new Directory(new File(pathCheckpoint))
    directoryCheckpoint.deleteRecursively()

    val eventB = Seq(
      Row("B", "b", 2022, 2, 6, "2022-02-06")
    )

    val eventBdf = spark.createDataFrame(
      spark.sparkContext.parallelize(eventB),
      StructType(someSchema)
    )
    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, eventBdf.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    val originalDateTime = LocalDateTime.now()
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val formatter = DateTimeFormatter.ofPattern(pattern)
    val newDateTime = originalDateTime.minus(10, ChronoUnit.SECONDS)
    val newDateTimeStr = newDateTime.format(formatter)

    val outputDf = new DeltaReader(delta_source_path, newDateTimeStr)
      .read(sqlCtx.sparkSession, EngineMode.Stream)
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", pathCheckpoint)
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .start(path)
    outputDf.awaitTermination(20000)

    eventually(timeout(Span(10, Seconds))) {
      val outputDf2 = DeltaReader(path)
        .read(sqlCtx.sparkSession, EngineMode.Batch)
      assertDataFrameNoOrderEquals(eventBdf, outputDf2)
    }

  }
}
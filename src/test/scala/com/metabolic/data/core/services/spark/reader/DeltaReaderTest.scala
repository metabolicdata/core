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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import scala.reflect.io.Directory
import java.io.File

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

  test("Read and Write event A into sink - hist mode") {
    val sqlCtx = sqlContext

    val directoryPath = new Directory(new File(path))
    directoryPath.deleteRecursively()

    val directoryCheckpoint = new Directory(new File(pathCheckpoint))
    directoryCheckpoint.deleteRecursively()

    val eventAdf = spark.createDataFrame(
      spark.sparkContext.parallelize(eventA),
      StructType(someSchema)
    )
    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, eventAdf.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    val outputDf = spark.readStream
      .option("startingTimestamp", "2000-01-01")
      .delta(delta_source_path)
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", pathCheckpoint)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(path)
    outputDf.awaitTermination(20000)

    eventually(timeout(Span(30, Seconds))) {
      val outputDf2 = DeltaReader(path)
        .read(sqlCtx.sparkSession, EngineMode.Batch)
      assertDataFrameNoOrderEquals(eventAdf, outputDf2)
    }

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

    write_into_source(eventBdf, delta_source_path, SaveMode.Append)
    val outputDf = DeltaReader(delta_source_path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)
    assertDataFrameNoOrderEquals(sourceDf, outputDf)
  }

  test("Read and Write event B into sink - without historical") {
    val sqlCtx = sqlContext

    val expectedData = Seq(
      Row("A", "a", 2022, 2, 5, "2022-02-05"),
      Row("B", "b", 2022, 2, 6, "2022-02-06")
    )
    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    val outputDf = spark.readStream
      .delta(delta_source_path)
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", pathCheckpoint)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(path)
    outputDf.awaitTermination(20000)

    eventually(timeout(Span(30, Seconds))) {
      val outputDf3 = DeltaReader(path)
        .read(sqlCtx.sparkSession, EngineMode.Batch)
      assertDataFrameNoOrderEquals(expectedDf, outputDf3)
    }
  }

  test("Add event C to the source") {
    val sqlCtx = sqlContext

    val eventC = Seq(
      Row("C", "c", 2022, 2, 7, "2022-02-07")
    )

    val sourceData = Seq(
      Row("A", "a", 2022, 2, 5, "2022-02-05"),
      Row("B", "b", 2022, 2, 6, "2022-02-06"),
      Row("C", "c", 2022, 2, 7, "2022-02-07")
    )
    val eventCdf = spark.createDataFrame(
      spark.sparkContext.parallelize(eventC),
      StructType(someSchema)
    )
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(sourceData),
      StructType(someSchema)
    )
    write_into_source(eventCdf, delta_source_path, SaveMode.Append)
    val outputDf = DeltaReader(delta_source_path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)
    assertDataFrameNoOrderEquals(sourceDf, outputDf)
  }

  test("Read and Write event C into sink - without historical") {
    val sqlCtx = sqlContext

    val expectedData = Seq(
      Row("A", "a", 2022, 2, 5, "2022-02-05"),
      Row("B", "b", 2022, 2, 6, "2022-02-06"),
      Row("C", "c", 2022, 2, 7, "2022-02-07"),
    )
    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    val outputDf = spark.readStream
      .delta(delta_source_path)
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", pathCheckpoint)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(path)
    outputDf.awaitTermination(20000)

    assertDataFrameNoOrderEquals(expectedDf, DeltaReader(path)
      .read(sqlCtx.sparkSession, EngineMode.Batch))

  }

  test("Read table with historical and checkpoint") {
    val sqlCtx = sqlContext

    val expectedData = Seq(
      Row("A", "a", 2022, 2, 5, "2022-02-05"),
      Row("B", "b", 2022, 2, 6, "2022-02-06"),
      Row("C", "c", 2022, 2, 7, "2022-02-07"),
    )
    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    val outputDf = spark.readStream
      .option("startingTimestamp", "2000-01-01")
      .delta(delta_source_path)
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", pathCheckpoint)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(path)
    outputDf.awaitTermination(20000)

    val outputDf3 = DeltaReader(path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)
    assertDataFrameNoOrderEquals(expectedDf, outputDf3)
  }

  test("Read table with historical and no checkpoint") {
    val sqlCtx = sqlContext

    val directoryCheckpoint = new Directory(new File(pathCheckpoint))
    directoryCheckpoint.deleteRecursively()

    val expectedData = Seq(
      Row("A", "a", 2022, 2, 5, "2022-02-05"),
      Row("A", "a", 2022, 2, 5, "2022-02-05"),
      Row("B", "b", 2022, 2, 6, "2022-02-06"),
      Row("B", "b", 2022, 2, 6, "2022-02-06"),
      Row("C", "c", 2022, 2, 7, "2022-02-07"),
      Row("C", "c", 2022, 2, 7, "2022-02-07"),
    )
    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    val outputDf = spark.readStream
       .option("startingTimestamp", "2000-01-01")
      .delta(delta_source_path)
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", pathCheckpoint)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(path)
    outputDf.awaitTermination(20000)

      val outputDf3 = DeltaReader(path)
        .read(sqlCtx.sparkSession, EngineMode.Batch)
      assertDataFrameNoOrderEquals(expectedDf, outputDf3)
  }
}

package com.metabolic.data.core.services.spark.writer

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.core.services.spark.writer.file.IcebergWriter
import com.metabolic.data.mapper.domain.io.{EngineMode, WriteMode}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

class IcebergWriterTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll {

  private val expectedData = Seq(
    Row("A", "a", 2022, 2, 5, "2022-02-05"),
    Row("B", "b", 2022, 2, 4, "2022-02-04"),
    Row("C", "c", 2022, 2, 3, "2022-02-03"),
    Row("D", "d", 2022, 2, 2, "2022-02-02"),
    Row("E", "e", 2022, 2, 1, "2022-02-01"),
    Row("F", "f", 2022, 1, 5, "2022-01-05"),
    Row("G", "g", 2021, 2, 2, "2021-02-02"),
    Row("H", "h", 2020, 2, 5, "2020-02-05")
  )

  private val upsertedData = Seq(
    Row("A", "a2", 2022, 2, 5, "2022-02-05"),
    Row("B", "b", 2022, 2, 4, "2022-02-04"),
    Row("C", "c2", 2022, 2, 3, "2022-02-03"),
    Row("D", "d", 2022, 2, 2, "2022-02-02"),
    Row("E", "e", 2022, 2, 1, "2022-02-01"),
    Row("F", "f", 2022, 1, 5, "2022-01-05"),
    Row("G", "g", 2021, 2, 2, "2021-02-02"),
    Row("H", "h", 2020, 2, 5, "2020-02-05"),
    Row("I", "i", 2021, 2, 6, "2021-02-06")
  )

  private val combinedData = Seq(
    Row("A", "a", 2022, 2, 5, "2022-02-05"),
    Row("A", "a2", 2022, 2, 5, "2022-02-05"),
    Row("B", "b", 2022, 2, 4, "2022-02-04"),
    Row("C", "c", 2022, 2, 3, "2022-02-03"),
    Row("C", "c2", 2022, 2, 3, "2022-02-03"),
    Row("D", "d", 2022, 2, 2, "2022-02-02"),
    Row("E", "e", 2022, 2, 1, "2022-02-01"),
    Row("F", "f", 2022, 1, 5, "2022-01-05"),
    Row("G", "g", 2021, 2, 2, "2021-02-02"),
    Row("H", "h", 2020, 2, 5, "2020-02-05"),
    Row("I", "i", 2021, 2, 6, "2021-02-06")
  )

  private val expectedSchema = List(
    StructField("name", StringType),
    StructField("data", StringType),
    StructField("yyyy", IntegerType),
    StructField("mm", IntegerType),
    StructField("dd", IntegerType),
    StructField("date", StringType)
  )

  private val differentData = Seq(
    Row("A", "a", 2022, 2, 5, "2022-02-05"),
    Row("B", "b", 2022, 2, 4, "2022-02-04"),
    Row("C", "c", 2022, 2, 3, "2022-02-03"),
    Row("D", "d", 2022, 2, 2, "2022-02-02"),
    Row("E", "e", 2022, 2, 1, "2022-02-01"),
    Row("F", "f", 2022, 1, 5, "2022-01-05"),
    Row("G", "g", 2021, 2, 2, "2021-02-02"),
    Row("H", "h", 2020, 2, 5, "2020-02-05")
  )

  private val differentSchema = List(
    StructField("name2", StringType),
    StructField("data2", StringType),
    StructField("yyyy2", IntegerType),
    StructField("mm2", IntegerType),
    StructField("dd2", IntegerType),
    StructField("date2", StringType)
  )

  val testDir = "./src/test/tmp/iw_test/"
  val catalog = "spark_catalog"
  val database = "data_lake"

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hadoop")
    .set("spark.sql.catalog.spark_catalog.warehouse", s"$testDir")
    .set("spark.sql.defaultCatalog", s"$catalog")
    .set("spark.sql.catalog.spark_catalog.default-namespace", "spark_catalog")

  private def createExpectedDataFrame(): DataFrame = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
  }

  private def createUpsertedDataFrame(): DataFrame = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.createDataFrame(
      spark.sparkContext.parallelize(upsertedData),
      StructType(expectedSchema)
    )
  }

  private def createCombinedDataFrame(): DataFrame = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.createDataFrame(
      spark.sparkContext.parallelize(combinedData),
      StructType(expectedSchema)
    )
  }

  private def createDifferentDataFrame(): DataFrame = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.createDataFrame(
      spark.sparkContext.parallelize(differentData),
      StructType(differentSchema)
    )
  }

  private def cleanUpTestDir(): Unit = {
    new Directory(new File(testDir)).deleteRecursively()
  }

  test("Iceberg batch append schema evolution") {
    cleanUpTestDir()
    val table = "letters_append_schema_change"
    val fqn = s"$catalog.$database.$table"
    val originalDF = createExpectedDataFrame() // e.g., has columns: data, name, id
    val modifiedDF = originalDF.withColumn("new_col", lit("new_value")) // extra column

    val wm = WriteMode.Append
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, None, cpl)(spark)

    iceberg.write(originalDF, EngineMode.Batch)
    val originalSchema = spark.table(fqn).schema

    iceberg.write(modifiedDF, EngineMode.Batch)
    val newSchema = spark.table(fqn).schema

    assert(!originalSchema.equals(newSchema), "Schema should have changed due to additional column.")
    assert(newSchema.fieldNames.contains("new_col"), "Expected new_col to be present in table schema.")
    cleanUpTestDir()
  }

  test("Iceberg batch append") {
    cleanUpTestDir()
    val table = "letters_append"
    val fqn = s"$catalog.$database.$table"
    val inputDF = createExpectedDataFrame()

    val wm = WriteMode.Append
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, None, cpl)(spark)

    iceberg.write(inputDF, EngineMode.Batch)
    iceberg.write(inputDF, EngineMode.Batch)

    val outputDf = spark.table(fqn)
    val expectedDf = inputDF.union(inputDF)

    assertDataFrameNoOrderEquals(expectedDf, outputDf)
    cleanUpTestDir()
  }

  test("Iceberg batch overwrite replace schema") {
    cleanUpTestDir()
    val table = "letters_overwrite_schema_change"
    val fqn = s"$catalog.$database.$table"
    val originalDF = createExpectedDataFrame() // e.g., has columns: data, name, id
    val modifiedDF = originalDF.withColumn("new_col", lit("new_value")) // extra column

    val wm = WriteMode.Overwrite
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, None, cpl)(spark)

    iceberg.write(originalDF, EngineMode.Batch)
    val originalSchema = spark.table(fqn).schema

    iceberg.write(modifiedDF, EngineMode.Batch)
    val newSchema = spark.table(fqn).schema

    assert(!originalSchema.equals(newSchema), "Schema should have changed due to additional column.")
    assert(newSchema.fieldNames.contains("new_col"), "Expected new_col to be present in table schema.")
    cleanUpTestDir()
  }

  test("Iceberg batch upsert wrong data") {
    cleanUpTestDir()
    val table = "letters_upsert_bad"
    val fqn = s"$catalog.$database.$table"
    val inputDF = createExpectedDataFrame()
    val differentInputDF = createDifferentDataFrame()

    val wm = WriteMode.Upsert
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, Some(Seq("data")), cpl)(spark)

    iceberg.write(inputDF, EngineMode.Batch)

    val exception = intercept[Exception] {
      iceberg.write(differentInputDF, EngineMode.Batch)
    }

    assert(exception.getMessage.contains("UNRESOLVED_COLUMN"))
    cleanUpTestDir()
  }

  test("Iceberg batch upsert") {
    cleanUpTestDir()
    val table = "letters_upsert"
    val fqn = s"$catalog.$database.$table"
    val inputDF = createExpectedDataFrame()
    val upsertedInputDF = createUpsertedDataFrame()
    val combinedDF = createCombinedDataFrame()

    val wm = WriteMode.Upsert
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, Some(Seq("data","name")), cpl)(spark)

    iceberg.write(inputDF, EngineMode.Batch)
    iceberg.write(inputDF, EngineMode.Batch)
    iceberg.write(upsertedInputDF, EngineMode.Batch)

    val outputDf = spark.table(fqn)

    assertDataFrameNoOrderEquals(combinedDF, outputDf)
    cleanUpTestDir()
  }

  test("Iceberg batch upsert schema evolution") {
    cleanUpTestDir()
    val table = "letters_upsert_schema_evolution"
    val fqn = s"$catalog.$database.$table"

    val initialDF = spark.createDataFrame(Seq(
      ("a", "apple"),
      ("b", "banana")
    )).toDF("data", "name")

    val upsertWithNewColumnDF = spark.createDataFrame(Seq(
      ("a", "apricot", "fruit"),
      ("c", "carrot", "vegetable")
    )).toDF("data", "name", "category")

    val expectedDF = spark.createDataFrame(Seq(
      ("a", "apricot", "fruit"),    // updated
      ("b", "banana", null),        // untouched, no category
      ("c", "carrot", "vegetable")  // new insert
    )).toDF("data", "name", "category")

    val wm = WriteMode.Upsert
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, Some(Seq("data")), cpl)(spark)

    iceberg.write(initialDF, EngineMode.Batch)
    iceberg.write(upsertWithNewColumnDF, EngineMode.Batch)

    val outputDf = spark.table(fqn)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)
    cleanUpTestDir()
  }

  test("Iceberg batch delete drops the table with purge") {
    cleanUpTestDir()
    val table = "letters_delete"
    val fqn = s"$catalog.$database.$table"
    val inputDF = createExpectedDataFrame()

    val writerAppend = new IcebergWriter(fqn, WriteMode.Append, None, "")(spark)
    writerAppend.write(inputDF, EngineMode.Batch)

    assert(spark.catalog.tableExists(fqn))

    val writerDelete = new IcebergWriter(fqn, WriteMode.Delete, None, "")(spark)
    writerDelete.write(inputDF, EngineMode.Batch)

    assert(!spark.catalog.tableExists(fqn))

    cleanUpTestDir()
  }

  test("Iceberg streaming append") {
    cleanUpTestDir()
    val expected = s"$catalog.$database.letters"
    val result = s"$catalog.$database.letters_result"

    val expectedDf = createExpectedDataFrame()

    expectedDf
      .write
      .format("iceberg")
      .mode("append")
      .saveAsTable(expected)

    val streamStartTimestamp = System.currentTimeMillis() - 3600000 // 1 hour ago
    val streamDf = spark.readStream
      .format("iceberg")
      .option("stream-from-timestamp", streamStartTimestamp.toString)
      .load(expected)

    val wm = WriteMode.Append
    val cpl = testDir + "checkpoints"
    val iceberg = new IcebergWriter(result, wm, None, cpl)(spark)
    iceberg.write(streamDf, EngineMode.Stream)

    //wait for the trigger to complete
    Thread.sleep(1000)

    assertDataFrameNoOrderEquals(expectedDf, spark.table(result))
    cleanUpTestDir()
  }

  //TODO: fill this test
  ignore("Iceberg streaming complete") {

  }

}
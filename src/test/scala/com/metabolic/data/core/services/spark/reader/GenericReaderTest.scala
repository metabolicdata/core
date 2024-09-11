package com.metabolic.data.core.services.spark.reader

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.core.services.spark.reader.table.GenericReader
import com.metabolic.data.mapper.domain.io.EngineMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

class GenericReaderTest extends AnyFunSuite
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
  private val expectedSchema = List(
    StructField("name", StringType, true),
    StructField("data", StringType, true),
    StructField("yyyy", IntegerType, true),
    StructField("mm", IntegerType, true),
    StructField("dd", IntegerType, true),
    StructField("date", StringType, true),
  )

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hive")
    .set("spark.databricks.delta.optimize.repartition.enabled","true")
    .set("spark.databricks.delta.vacuum.parallelDelete.enabled","true")
    .set("spark.databricks.delta.retentionDurationCheck.enabled","false")
    .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.local.type", "hadoop")
    .set("spark.sql.catalog.local.warehouse", "src/test/tmp/it_formats")
    .set("spark.sql.defaultCatalog", "spark_catalog")


  test("Iceberg batch read") {

    val fqn = "local.data_lake.letters"

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    expectedDf
      .write
      .format("iceberg")
      .mode("overwrite")
      .saveAsTable(fqn)

    val iceberg = new GenericReader(fqn)
    val inputDf = iceberg.read(spark, EngineMode.Batch)

    assertDataFrameEquals(inputDf, expectedDf)
  }

  //TODO: Implement this test
  ignore("Delta batch read") {

  }

  test("Iceberg stream read") {

    new Directory(new File("./warehouse/data_lake/letters")).deleteRecursively()
    new Directory(new File("src/test/tmp/checkpoints")).deleteRecursively()
    new Directory(new File("src/test/tmp/parquet")).deleteRecursively()

    val fqn = "local.data_lake.letters"

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    expectedDf
      .write
      .format("iceberg")
      .mode("append")
      .saveAsTable(fqn)

    val iceberg = new GenericReader(fqn)
    val inputDf = iceberg.read(spark, EngineMode.Stream)

    val checkpointPath = "src/test/tmp/checkpoints"

    val query = inputDf.writeStream
      .format("parquet") // or "csv", "json", etc.
      .outputMode("append") // Ensure the output mode is correct for your use case
      .trigger(Trigger.Once()) // Process only one batch
      .option("checkpointLocation", checkpointPath)
      .option("path", "src/test/tmp/parquet") // Specify the output path for the file
      .start()

    query.awaitTermination()

    val df = spark.read
      .format("parquet")
      .load("src/test/tmp/parquet")

    assertDataFrameEquals(df, expectedDf)

  }

  //TODO: Implement this test
  ignore("Delta stream read") {

  }

}

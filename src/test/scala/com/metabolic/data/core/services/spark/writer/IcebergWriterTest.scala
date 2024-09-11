package com.metabolic.data.core.services.spark.writer

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.core.services.spark.writer.file.IcebergWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import com.metabolic.data.mapper.domain.io.{EngineMode, WriteMode}

import java.io.File
import scala.reflect.io.Directory

class IcebergWriterTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll {

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hive")
    .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.local.type", "hadoop")
    .set("spark.sql.catalog.local.warehouse", "./warehouse")
    .set("spark.sql.defaultCatalog", "local")


  private val inputData = Seq(
    Row("A", "a", 2022, 2, 5, "2022-02-05"),
    Row("B", "b", 2022, 2, 4, "2022-02-04"),
    Row("C", "c", 2022, 2, 3, "2022-02-03"),
    Row("D", "d", 2022, 2, 2, "2022-02-02"),
    Row("E", "e", 2022, 2, 1, "2022-02-01"),
    Row("F", "f", 2022, 1, 5, "2022-01-05"),
    Row("G", "g", 2021, 2, 2, "2021-02-02"),
    Row("H", "h", 2020, 2, 5, "2020-02-05")
  )

  private val someSchema = List(
    StructField("name", StringType, true),
    StructField("data", StringType, true),
    StructField("yyyy", IntegerType, true),
    StructField("mm", IntegerType, true),
    StructField("dd", IntegerType, true),
    StructField("date", StringType, true),
  )

  test("Iceberg batch append") {

    new Directory(new File("./warehouse/data_lake/letters_append")).deleteRecursively()

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val fqn = "local.data_lake.letters_append"
    val wm = WriteMode.Append
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, cpl)(spark)

    iceberg.write(inputDF, EngineMode.Batch)
    iceberg.write(inputDF, EngineMode.Batch)

    val outputDf = spark.table(fqn)

    val expectedDf = inputDF.union(inputDF)

    assertDataFrameNoOrderEquals(expectedDf, outputDf)

  }

  test("Iceberg batch overwrite") {

    new Directory(new File("./warehouse/data_lake/letters_append")).deleteRecursively()

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val fqn = "local.data_lake.letters_overwrite"
    val wm = WriteMode.Overwrite
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, cpl)(spark)

    iceberg.write(inputDF, EngineMode.Batch)

    val outputDf = spark.table(fqn)

    assertDataFrameNoOrderEquals(inputDF, outputDf)

  }

  //TODO: Implement this test using Kafka
  ignore("Iceberg streaming append") {

    new Directory(new File("./warehouse/data_lake/letters_input")).deleteRecursively()
    new Directory(new File("./warehouse/data_lake/letters_output")).deleteRecursively()

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val emptyDF = spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(someSchema)
    )

    val inputTableFqn = "local.data_lake.letters_input"
    val outputTableFqn = "local.data_lake.letters_output"
    val wm = WriteMode.Append
    val cpl = "./warehouse/_checkpoint"

    inputDF.write
      .format("iceberg")
      .mode("overwrite")
      .saveAsTable(inputTableFqn)

    emptyDF.write
      .format("iceberg")
      .mode("overwrite")
      .saveAsTable(outputTableFqn)

    val streamingInputDF = spark.readStream
      .format("iceberg")
      .option("path", inputTableFqn) // Specify the input Iceberg table path
      .load()

    val iceberg = new IcebergWriter(outputTableFqn, wm, cpl)(spark)
    iceberg.write(streamingInputDF, EngineMode.Stream)

    val outputDf = spark.table(outputTableFqn)
    assertDataFrameNoOrderEquals(inputDF, outputDf)
  }

  //TODO: fill this test
  ignore("Iceberg streaming complete") {

  }


}

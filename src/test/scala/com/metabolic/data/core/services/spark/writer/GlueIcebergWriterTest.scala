package com.metabolic.data.core.services.spark.writer

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.core.services.spark.writer.file.IcebergWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.run.EngineMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

class GlueIcebergWriterTest extends AnyFunSuite
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

  //TODO: how can we fill this secrets to test in AWS? Should we test only in local?
  val accessKey = "***"
  val secretKey = "***"
  val testDir = "src/test/tmp/iw_test/"
  val testBucket = "s3://factorial-datalake-iceberg-bronze-data/"

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .set("spark.sql.catalog.spark_catalog.warehouse", s"$testBucket")
    .set("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .set("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .set("spark.sql.catalog.spark_catalog.client.region", "eu-central-1")
    .set("spark.sql.defaultCatalog", "spark_catalog")

  System.setProperty("aws.accessKeyId", s"$accessKey")
  System.setProperty("aws.secretAccessKey", s"$secretKey")

  private def createExpectedDataFrame(): DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
  }

  private def cleanUpTestDir(): Unit = {
    new Directory(new File(testDir)).deleteRecursively()
  }

  ignore("Iceberg batch append") {
    cleanUpTestDir()
    val inputDF = createExpectedDataFrame()

    val fqn = "spark_catalog.default.letters_append"
    val wm = WriteMode.Append
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, cpl)(spark)

    iceberg.write(inputDF, EngineMode.Batch)

    val outputDf = spark.table(fqn)

    assertDataFrameNoOrderEquals(inputDF, outputDf)
  }

  ignore("Iceberg batch overwrite") {
    cleanUpTestDir()
    val inputDF = createExpectedDataFrame()

    val fqn = "spark_catalog.default.letters_overwrite"
    val wm = WriteMode.Overwrite
    val cpl = ""
    val iceberg = new IcebergWriter(fqn, wm, cpl)(spark)

    iceberg.write(inputDF, EngineMode.Batch)

    val outputDf = spark.table(fqn)

    assertDataFrameNoOrderEquals(inputDF, outputDf)
  }

  //TODO: review why writer is not writing
  ignore("Iceberg streaming append") {
    cleanUpTestDir()
    val expected = "local.data_lake.letters"
    val result = "local.data_lake.letters_result"
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
    val iceberg = new IcebergWriter(result, wm, cpl)(spark)

    //wait for the trigger to complete
    Thread.sleep(1000)

    iceberg.write(streamDf, EngineMode.Stream)

    assertDataFrameNoOrderEquals(expectedDf, spark.table(result))
  }

  //TODO: fill this test
  ignore("Iceberg streaming complete") {
    cleanUpTestDir()
  }

}

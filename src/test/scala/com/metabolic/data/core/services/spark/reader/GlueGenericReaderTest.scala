package com.metabolic.data.core.services.spark.reader

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.core.services.spark.reader.table.GenericReader
import com.metabolic.data.mapper.domain.io.EngineMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

class GlueGenericReaderTest extends AnyFunSuite
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
  val testDir = "src/test/tmp/gr_test/"
  val testBucket = "s3://factorial-datalake-iceberg-bronze-data/"

  //TODO: check iceberg catalog generalization
  //TODO: use same table for all tests
  //adding org.apache.spark.sql.delta.catalog.DeltaCatalog works with delta
  override def conf: SparkConf = super.conf
    .set("spark.databricks.delta.optimize.repartition.enabled", "true")
    .set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
    .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .set("spark.sql.catalog.glue_catalog.warehouse", s"$testBucket")
    .set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .set("spark.sql.catalog.glue_catalog.client.region", "eu-central-1")
    .set("spark.sql.defaultCatalog", "spark_catalog")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.catalogImplementation", "hive")
    .set("spark.sql.warehouse.dir", s"${testBucket}default_delta/warehouse")
    .set("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.hadoop.fs.s3a.access.key", s"$accessKey")
    .set("spark.hadoop.fs.s3a.secret.key", s"$secretKey")
    .set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

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

  ignore("Iceberg batch read") {
    cleanUpTestDir()
    val fqn = "glue_catalog.default.letters"
    spark.sql("CREATE DATABASE IF NOT EXISTS default")

    val expectedDf = createExpectedDataFrame()
    expectedDf
      .write
      .format("iceberg")
      .mode("overwrite")
      .saveAsTable(fqn)

    val iceberg = new GenericReader(fqn)
    val inputDf = iceberg.read(spark, EngineMode.Batch)

    assertDataFrameEquals(inputDf, expectedDf)
  }

  ignore("Delta batch read") {
    cleanUpTestDir()

    val fqn = "spark_catalog.default_delta.letters"
    spark.sql("CREATE DATABASE IF NOT EXISTS default_delta")

    val additional_options = Map(
      "path" -> "s3a://factorial-datalake-iceberg-bronze-data/default_delta/letters"
    )

    val expectedDf = createExpectedDataFrame()
    expectedDf
      .write
      .options(additional_options)
      .format("delta")
      .mode("overwrite")
      .saveAsTable(fqn)

    val delta = new GenericReader(fqn)
    val resultDf = delta.read(spark, EngineMode.Batch)

    val sortedExpectedDf = expectedDf.orderBy("name")
    val sortedResultDf = resultDf.orderBy("name")

    assertDataFrameEquals(sortedExpectedDf, sortedResultDf)
  }

  ignore("Iceberg stream read") {
    cleanUpTestDir()

    val fqn = "glue_catalog.default.letters"
    spark.sql("CREATE DATABASE IF NOT EXISTS default")
    val table = "letters"

    val expectedDf = createExpectedDataFrame()
    expectedDf
      .write
      .format("iceberg")
      .mode("overwrite")
      .saveAsTable(fqn)


    val iceberg = new GenericReader(fqn)
    val readDf = iceberg.read(spark, EngineMode.Stream)

    val checkpointPath = "s3a://factorial-datalake-iceberg-bronze-data/default_delta/checkpoints"

    val query = readDf.writeStream
      .format("parquet")
      .outputMode("append")
      .trigger(Trigger.Once())
      .option("checkpointLocation", checkpointPath)
      .option("path", testDir + table)
      .start()

    query.awaitTermination()

    val resultDf = spark.read
      .format("parquet")
      .load(testDir + table)

    assertDataFrameEquals(expectedDf, resultDf)

    expectedDf
      .write
      .format("iceberg")
      .mode("append")
      .saveAsTable(fqn)

    val query2 = readDf.writeStream
      .format("parquet") // or "csv", "json", etc.
      .outputMode("append") // Ensure the output mode is correct for your use case
      .trigger(Trigger.Once()) // Process only one batch
      .option("checkpointLocation", checkpointPath)
      .option("path", testDir + table) // Specify the output path for the file
      .start()

    query2.awaitTermination()

    val resultDf2 = spark.read
      .format("parquet")
      .load(testDir + table)

    assertDataFrameEquals(expectedDf.union(expectedDf), resultDf2)
  }

  ignore("Delta stream read") {
    cleanUpTestDir()

    new Directory(new File(testDir)).deleteRecursively()

    val fqn = "data_lake.letters_stream"
    val database = "data_lake"
    val table = "letters_stream"

    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${database}")

    val expectedDf = createExpectedDataFrame()
    expectedDf
      .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(fqn)

    val delta = new GenericReader(fqn)
    val inputDf = delta.read(spark, EngineMode.Stream)

    val checkpointPath = testDir + "checkpoints"

    val query = inputDf.writeStream
      .format("parquet") // or "csv", "json", etc.
      .outputMode("append") // Ensure the output mode is correct for your use case
      .trigger(Trigger.Once()) // Process only one batch
      .option("checkpointLocation", checkpointPath)
      .option("path", testDir + table) // Specify the output path for the file
      .start()
    query.awaitTermination()

    val resultDf = spark.read
      .format("parquet")
      .load(testDir + table)

    assertDataFrameNoOrderEquals(expectedDf, resultDf)
  }

  //TODO: test other formats and glue catalog compatibility

}

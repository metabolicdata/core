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

  val testDir = "./src/test/tmp/gr_test/"
  val testDir2 = "./src/test/tmp/gr_test2/"

  //TODO: check iceberg catalog generalization
  //TODO: use same table for all tests
  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hive")
    .set("spark.databricks.delta.optimize.repartition.enabled","true")
    .set("spark.databricks.delta.vacuum.parallelDelete.enabled","true")
    .set("spark.databricks.delta.retentionDurationCheck.enabled","false")
    .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.local.type", "hadoop")
    .set("spark.sql.catalog.local.warehouse", s"$testDir")
    .set("spark.sql.catalog.local2", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.local2.type", "hadoop")
    .set("spark.sql.catalog.local2.warehouse", s"$testDir2")
    .set("spark.sql.defaultCatalog", "spark_catalog")


  private def createExpectedDataFrame(): DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
  }

  private def cleanUpTestDir(): Unit = {
    new Directory(new File(testDir)).deleteRecursively()
  }

  test("Iceberg batch read") {
    cleanUpTestDir()
    val table = "letters_iceberg"
    val database = "data_lake"
    val fqn = s"local2.$database.$table"
    spark.sql("CREATE DATABASE IF NOT EXISTS local2.data_lake")

    val expectedDf = createExpectedDataFrame()
    expectedDf
      .write
      .format("iceberg")
      .mode("overwrite")
      .saveAsTable(fqn)

    val iceberg = new GenericReader(fqn)
    val inputDf = iceberg.read(spark, EngineMode.Batch)

    assertDataFrameEquals(inputDf, expectedDf)
    cleanUpTestDir()
  }

  test("Delta batch read") {
    cleanUpTestDir()
    val table = "letters_delta_batch"
    val database = "data_lake"
    val fqn = s"$database.$table"
    spark.sql("CREATE DATABASE IF NOT EXISTS data_lake")

    val expectedDf = createExpectedDataFrame()
    expectedDf
      .write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(fqn)

    val delta = new GenericReader(fqn)
    val resultDf = delta.read(spark, EngineMode.Batch)

    val sortedExpectedDf = expectedDf.orderBy("name")
    val sortedResultDf = resultDf.orderBy("name")

    assertDataFrameEquals(sortedExpectedDf, sortedResultDf)
    cleanUpTestDir()
  }

  test("Iceberg stream read") {
    cleanUpTestDir()
    val table = "letters_iceberg_stream"
    val database = "data_lake"
    val fqn = s"local.$database.$table"
    spark.sql("CREATE DATABASE IF NOT EXISTS data_lake")

    val expectedDf = createExpectedDataFrame()
    expectedDf
      .write
      .format("iceberg")
      .mode("append")
      .saveAsTable(fqn)

    val iceberg = new GenericReader(fqn)
    val readDf = iceberg.read(spark, EngineMode.Stream)

    val checkpointPath = testDir + "checkpoints"

    val query = readDf.writeStream
      .format("parquet")
      .outputMode("append")
      .trigger(Trigger.AvailableNow())
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
      .format("parquet")
      .outputMode("append")
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpointPath)
      .option("path", testDir + table)
      .start()

    query2.awaitTermination()

    val resultDf2 = spark.read
      .format("parquet")
      .load(testDir + table)

    assertDataFrameEquals(expectedDf.union(expectedDf), resultDf2)
    cleanUpTestDir()
  }

  test("Delta stream read") {
    cleanUpTestDir()
    val table = "letters_delta_stream"
    val database = "data_lake"
    val fqn = s"$database.$table"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")

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
      .format("parquet")
      .outputMode("append")
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpointPath)
      .option("path", testDir + table)
      .start()
    query.awaitTermination()

    val resultDf = spark.read
      .format("parquet")
      .load(testDir + table)

    val sortedExpectedDf = expectedDf.orderBy("name")
    val sortedResultDf = resultDf.orderBy("name")

    assertDataFrameEquals(sortedExpectedDf, sortedResultDf)
    cleanUpTestDir()
  }

  //TODO: test other formats and glue catalog compatibility
}
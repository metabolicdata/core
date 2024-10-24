package com.metabolic.data.mapper.app

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.util.ConfigReaderService
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.services.SourceConfigParserService
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import com.metabolic.data.mapper.domain.ops.source._
import org.apache.derby.impl.sql.compile.TableName

class MetabolicReaderIT extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

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

  def getFakeEmployeesDataframe(): DataFrame = {

    val fakeEmployeesData = Seq(
      Row("Marc", 33L, 1),
      Row("Pau", 30L, 1)
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("age", LongType, true),
      StructField("version", IntegerType, true)
    )

    spark.createDataFrame(
      spark.sparkContext.parallelize(fakeEmployeesData),
      StructType(someSchema)
    )

  }

  def getFileSource(inputPath: String, tableName: String, format: String): Seq[Source] = {

    val genericSourceHOCON = {
      """sources: [
        |  {
        |    inputPath = "$inputPath"
        |    name = "$tableName"
        |    format = "$format"
        |  }
        |]
        |""".stripMargin
        .replace("$inputPath", inputPath)
        .replace("$tableName", tableName)
        .replace("$format", format)
    }

    val sourceConfig = new ConfigReaderService()
      .loadConfig(genericSourceHOCON, "")

    SourceConfigParserService()
      .parseSources(sourceConfig)

  }

  test("Reader Parquet Batch") {

    val inputPath = "src/test/tmp/fake_employee_parquet"
    val tableName = "employees"

    val expected = getFakeEmployeesDataframe()

    expected
      .write
      .mode("overwrite")
      .parquet(inputPath)

    val source = getFileSource(inputPath, tableName, IOFormat.PARQUET.toString).head

    MetabolicReader.read(source, true, EngineMode.Batch, false, "", "")(spark)

    val result = spark.table(tableName)

    assertDataFrameNoOrderEquals(expected, result)

  }

  test("Reader Json Batch") {

    val inputPath = "src/test/tmp/fake_employee_json"
    val tableName = "employees"

    val expected = getFakeEmployeesDataframe()
      .select(col("age").cast(LongType),
        col("name").cast(StringType),
        col("version").cast(LongType))

    expected
      .write
      .mode("overwrite")
      .json(inputPath)

    val source = getFileSource(inputPath, tableName, IOFormat.JSON.toString).head

    MetabolicReader.read(source, true, EngineMode.Batch, false, "", "")(spark)

    val result = spark.table(tableName)

    assertDataFrameNoOrderEquals(expected, result)

  }

  test("Reader CSV Batch") {

    val inputPath = "src/test/tmp/fake_employee_csv"
    val tableName = "employees"

    val expected = getFakeEmployeesDataframe()
          .select(col("age").cast(StringType),
            col("name").cast(StringType),
            col("version").cast(StringType))

    expected
      .write
      .option("header",true)
      .mode("overwrite")
      .csv(inputPath)

    val source = getFileSource(inputPath, tableName, IOFormat.CSV.toString).head

    MetabolicReader.read(source, true, EngineMode.Batch, false, "", "")(spark)

    val result = spark.table(tableName)

    assertDataFrameNoOrderEquals(expected, result)

  }

  test("Reader Delta Batch") {

    val inputPath = "src/test/tmp/fake_employee_delta"
    val tableName = "employees"

    val expected = getFakeEmployeesDataframe()

    expected
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(inputPath)

    val source = getFileSource(inputPath, tableName, IOFormat.DELTA.toString).head

    MetabolicReader.read(source, historical = true, EngineMode.Batch, enableJDBC = false, "", "")(spark)

    val result = spark.table(tableName)

    assertDataFrameNoOrderEquals(expected, result)

  }

  test("Reader Table Iceberg Batch") {

    val catalog = "local.data_lake"
    val tableName = "fake_employee_delta"
    val fqn = catalog + "." + tableName

    val expected = getFakeEmployeesDataframe()

    expected
      .write
      .format("iceberg")
      .mode("overwrite")
      .saveAsTable(fqn)

    val source = TableSource(fqn,tableName)

    MetabolicReader.read(source, historical = true, EngineMode.Batch, enableJDBC = false, "", "")(spark)

    val result = spark.table(tableName)

    assertDataFrameNoOrderEquals(expected, result)

  }

  // This test is not doing a full comparison
  ignore("Reader Table Delta Batch") {

    val inputPath = "src/test/tmp/fake_employee_delta"
    val catalog = "default"
    val tableName = "fake_employee_delta"

    val expected = getFakeEmployeesDataframe()

    expected
      .write
      .format("delta")
      .mode("overwrite")
      .option("mergeSchema", "true")
      .save(inputPath)

    spark.sql(s"""
      CREATE TABLE default.fake_employee_delta
      USING DELTA
      LOCATION '$inputPath'
    """)

    val fqn = catalog + "." + tableName

    val source = TableSource(fqn,tableName)

    MetabolicReader.read(source, historical = true, EngineMode.Batch, enableJDBC = false, "", "")(spark)

    val result = spark.table(fqn)

    assertDataFrameNoOrderEquals(expected, result)

  }

}

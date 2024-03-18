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


class MetabolicReaderIT extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.databricks.delta.optimize.repartition.enabled","true")
    .set("spark.databricks.delta.vacuum.parallelDelete.enabled","true")
    .set("spark.databricks.delta.retentionDurationCheck.enabled","false")

  def getFakeEmployeesDataframe(): DataFrame = {

    val sqlCtx = sqlContext

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


}

package com.metabolic.data.mapper.app

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops.SQLStatmentMapping
import com.metabolic.data.mapper.domain.{Config, io}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FileFormatsIT extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hive")
    .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.local.type", "hadoop")
    .set("spark.sql.catalog.local.warehouse", "src/test/tmp/it_formats")
    .set("spark.sql.defaultCatalog", "local")

  test("Write parquet") {

    val sqlCtx = sqlContext

    val fakeEmployeesData = Seq(
      Row("Marc", 33, 1),
      Row("Pau", 30, 1)
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("version", IntegerType, true)
    )

    val fakeEmployeesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeEmployeesData),
      StructType(someSchema)
    )

    fakeEmployeesDF.write
      .mode("overwrite")
      .parquet("src/test/tmp_formats/fake_parquet_employees")

    val multilineSQL = """|select *
                          |from employees""".stripMargin

    val testingConfig = Config(
      "",
      List(io.FileSource("src/test/tmp_formats/fake_parquet_employees", "employees", IOFormat.PARQUET)),
      List(SQLStatmentMapping(multilineSQL)),
      FileSink("test", "src/test/tmp_formats/il_fake_parquet_employees_t", WriteMode.Overwrite, IOFormat.PARQUET)
    )

    MetabolicApp()
      .transformAll(List(testingConfig))(region, spark)

    val NT_fakeEmployeesDF = spark.read
      .parquet("src/test/tmp_formats/il_fake_parquet_employees_t")

    assertDataFrameNoOrderEquals(fakeEmployeesDF, NT_fakeEmployeesDF)
  }

  test("Write json") {

    val sqlCtx = sqlContext

    val fakeEmployeesData = Seq(
      Row("Marc", 33L),
      Row("Pau", 30L)
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("age", LongType, true)
    )

    val fakeEmployeesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeEmployeesData),
      StructType(someSchema)
    )

    fakeEmployeesDF
      .repartition(1)
      .write
      .mode("overwrite")
      .json("src/test/tmp_formats/fake_json_employees")

    val multilineSQL = "select name, cast(age as long) as age from employees"

    val testingConfig = Config(
      "",
      List(FileSource("src/test/tmp_formats/fake_json_employees", "employees", format = IOFormat.JSON)),
      List(SQLStatmentMapping(multilineSQL)),
      io.FileSink("test", "src/test/tmp_formats/il_fake_json_employees_t", WriteMode.Overwrite, IOFormat.JSON)
    )

    MetabolicApp()
      .transformAll(List(testingConfig))(region, spark)

    val NT_fakeEmployeesDF = spark.read
      .json("src/test/tmp_formats/il_fake_json_employees_t")

    assertDataFrameNoOrderEquals(
      fakeEmployeesDF.select("name", "age"),
      NT_fakeEmployeesDF.select("name", "age")
    )
  }

  test("Read json table") {

    val sqlCtx = sqlContext

    val fakeEmployeesData = Seq(
      Row("Marc", 33L),
      Row("Pau", 30L)
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("age", LongType, true)
    )

    val inputEmployeesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeEmployeesData),
      StructType(someSchema)
    )

    inputEmployeesDF.write.saveAsTable("fake_json_employees")

    print(spark.catalog.tableExists("fake_json_employees"))

    val multilineSQL = "select name, cast(age as string) as new_age from employees"

    val testingConfig = Config(
      "",
      List(TableSource("fake_json_employees", "employees")),
      List(SQLStatmentMapping(multilineSQL)),
      io.FileSink("test", "src/test/tmp_formats/table_fake_json_employees_t", WriteMode.Overwrite, IOFormat.JSON)
    )

    MetabolicApp()
      .transformAll(List(testingConfig))(region, spark)

    val NT_fakeEmployeesDF = spark.read
      .json("src/test/tmp_formats/table_fake_json_employees_t")


    val expectedEmployeesData = Seq(
      Row("Marc", "33"),
      Row("Pau", "30")
    )

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("new_age", StringType, true)
    )

    val expectedEmployeeDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedEmployeesData),
      StructType(expectedSchema)
    )

    assertDataFrameNoOrderEquals(
      expectedEmployeeDF.select("name", "new_age"),
      NT_fakeEmployeesDF.select("name", "new_age")
    )
  }

  test("Write table") {

    val inputEmployeesData = Seq(
      Row("Marc", "33"),
      Row("Pau", "30")
    )

    val inputSchema = List(
      StructField("name", StringType, true),
      StructField("age", StringType, true)
    )

    val inputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(inputEmployeesData),
      StructType(inputSchema)
    )

    val fqnInput = "local.data_lake.iceberg_employees"

    inputDf
      .write
      .format("iceberg")
      .mode("overwrite")
      .saveAsTable(fqnInput)


    val expectedEmployeesData = Seq(
      Row("Marc", 33L),
      Row("Pau", 30L)
    )

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("age", LongType, true)
    )

    val fqnOutput = "local.data_lake.iceberg_employees_long"

    val multilineSQL = "select name, cast(age as long) as age from employees"

    val testingConfig = Config(
      "",
      List(TableSource(fqnInput, "employees")),
      List(SQLStatmentMapping(multilineSQL)),
      TableSink("test", fqnOutput, WriteMode.Overwrite, List.empty)
    )

    MetabolicApp()
      .transformAll(List(testingConfig))(region, spark)

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedEmployeesData),
      StructType(expectedSchema)
    )

    val outputDf = spark.table(fqnOutput)


    assertDataFrameNoOrderEquals(
      expectedDf //.select("name", "age")
      ,outputDf //.select("name", "age")
    )
  }

  test("Write delta") {

    val inputEmployeesData = Seq(
      Row("Marc", "33"),
      Row("Pau", "30")
    )

    val inputSchema = List(
      StructField("name", StringType, true),
      StructField("age", StringType, true)
    )

    val inputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(inputEmployeesData),
      StructType(inputSchema)
    )

    val inputPath = "src/test/tmp/it_formats/data_lake/delta_employees"

    inputDf
      .write
      .mode("overwrite")
      .format("delta")
      .save(inputPath)


    val expectedEmployeesData = Seq(
      Row("Marc", 33L),
      Row("Pau", 30L)
    )

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("age", LongType, true)
    )

    val outputPath = "src/test/tmp/it_formats/data_lake/delta_employees_long"
    val multilineSQL = "select name, cast(age as long) as age from employees"

    val testingConfig = Config(
      "",
      List(FileSource(inputPath, "employees", format = IOFormat.DELTA)),
      List(SQLStatmentMapping(multilineSQL)),
      FileSink("test", outputPath, WriteMode.Overwrite, IOFormat.DELTA, Option("name"), Option("age"), ops = List.empty)
    )

    MetabolicApp()
      .transformAll(List(testingConfig))(region, spark)

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedEmployeesData),
      StructType(expectedSchema)
    )

    val outputDf = spark.read.format("delta") .load(outputPath)

    assertDataFrameNoOrderEquals(expectedDf, outputDf)
  }

}

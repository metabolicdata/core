package com.metabolic.data.mapper.app

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.domain.Environment
import com.metabolic.data.core.services.util.ConfigReaderService
import com.metabolic.data.mapper.domain.io.{EngineMode, IOFormat, Sink}
import com.metabolic.data.mapper.services.SinkConfigParserService
import io.delta.implicits._
import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MetabolicWriterIT extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest{

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  def getFakeEmployeesDataframe(withHeaders: Boolean = false): DataFrame = {

    val sqlCtx = sqlContext

    val fakeEmployeesData = Seq(
      Row("Marc", 33L, "2020-02-01T00:00:00.000Z", 2020, 2, 1, 1),
      Row("Marc", 35L, "2022-12-22T00:00:00.000Z", 2022, 12, 22, 1),
      Row("Pau" , 30L, "2022-10-01T00:00:00.000Z", 2022, 10, 1, 1)
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("age", LongType, true),
      StructField("updated_at", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("version", IntegerType, true)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeEmployeesData),
      StructType(someSchema)
    )

    df
      .withColumn("updated_at", col("updated_at").cast(TimestampType))

  }

  def getFileSink(outputPath: String, tableName: String, format: String, mode: String): Sink = {

    val genericSourceHOCON = {
      """sink:
        |  {
        |    path = "outputPath"
        |    name = "$tableName"
        |    format = "$format"
        |    eventDtColumn = "updated_at"
        |    writeMode = "$mode"
        |    idColumn = "name"
        |    mode = "replace"
        |    ops: [
        |       {
        |          op: date_partition
        |          eventDtColumn = "updated_at"
        |       }
        |    ]
        |  }
        |""".stripMargin
        .replace("outputPath", outputPath)
        .replace("$tableName", tableName)
        .replace("$format", format)
        .replace("$mode", mode)
    }

    val fileSinkConfig = new ConfigReaderService()
      .loadConfig(genericSourceHOCON, "")

    SinkConfigParserService()
      .parseSink(fileSinkConfig, Environment("",EngineMode.Batch, "", false,"dbName","", region, Option.empty, Option.empty, Option.empty, autoSchema = true))

  }

  test("Write Parquet Batch") {

    val outputPath = "src/test/tmp/out_fake_employee_parquet"
    val tableName = "employees"

    val sink = getFileSink(outputPath, tableName, IOFormat.PARQUET.toString, "append")

    val expected = getFakeEmployeesDataframe()
      .selectExpr("name","age","updated_at", "yyyy", "mm", "dd", "version")

    val input = expected
      .select(
        col("name"),
        col("age"),
        col("updated_at"),
        col("version")
      )

    MetabolicWriter
      .write(input,  sink, true, false, "", EngineMode.Batch, Seq.empty[String])(spark, region)

    val result = spark.read.parquet(outputPath)
      .selectExpr("name","age","updated_at", "yyyy", "mm", "dd", "version")

    assertDataFrameNoOrderEquals(expected, result)

  }

  test("Write Json Batch") {

    val outputPath = "src/test/tmp/out_fake_employee_json"
    val tableName = "employees"

    val sink = getFileSink(outputPath, tableName, IOFormat.JSON.toString, "append")

    val expected = getFakeEmployeesDataframe()
      .selectExpr("name","age","updated_at", "version", "yyyy", "mm", "dd" )

    val input = expected
      .select(
        col("name"),
        col("age"),
        col("updated_at"),
        col("version")
      )

    MetabolicWriter.write(input, sink, true, false, "", EngineMode.Batch, Seq.empty[String])(spark, region)

    val result = spark.read
      //.option("timestampFormat","yyyy-MM-dd HH:mm:ss.SSSXXX")
      .json(outputPath)
      .selectExpr("name","age",
        "cast(updated_at as timestamp)", //https://issues.apache.org/jira/browse/SPARK-26325 Unresolved for scala
        "cast(version as integer)", "yyyy", "mm", "dd" )

    assertDataFrameNoOrderEquals(expected, result)

  }

  test("Write CSV Batch") {

    val outputPath = "src/test/tmp/out_fake_employee_csv"
    val tableName = "employees"

    val sink = getFileSink(outputPath, tableName, IOFormat.CSV.toString, "append")

    val expected = getFakeEmployeesDataframe()
      .selectExpr("name","age","updated_at", "yyyy", "mm", "dd", "version")

    val input = expected
      .select(
        col("name"),
        col("age"),
        col("updated_at"),
        col("version")
      )

    MetabolicWriter.write(input, sink, true, false, "", EngineMode.Batch, Seq.empty[String] )(spark, region)

    val result = spark.read
      .option("header", true)
      .csv(outputPath)
      .selectExpr("name",
        "cast(age as long)",
        "cast(updated_at as timestamp)",
        "yyyy", "mm", "dd", "cast(version as integer)")

    assertDataFrameNoOrderEquals(expected, result)

  }

  test("Write Delta Batch - Append") {
    val outputPath = "src/test/tmp/delta/out_fake_employees_delta_append"
    val tableName = "employees_append_table"

    val sink = getFileSink(outputPath, tableName, IOFormat.DELTA.toString, "append")

    val expected = getFakeEmployeesDataframe()
      .selectExpr("name","age","updated_at", "yyyy", "mm", "dd", "version")

    val input = expected
      .select(
        col("name"),
        col("age"),
        col("updated_at"),
        col("version")
      )

    if (!DeltaTable.isDeltaTable(outputPath)) {
      try {
        val deltaTable = DeltaTable.createIfNotExists()
          .tableName(tableName)
          .location(outputPath)
          .addColumns(input.schema)
          .execute()

        if (DeltaTable.isDeltaTable(outputPath)) {
          println(s"Table $tableName created successfully.")
        } else {
          println("Table creation failed, it may be in an incomplete state.")
        }
      } catch {
        case e: Exception =>
          println(s"Error creating Delta table: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    try {
      println("Appending data to Delta table.")
      val deltaTable = DeltaTable.forPath(outputPath) // Use forPath to reference the existing table
      deltaTable.toDF.write.format("delta").mode(SaveMode.Append).save(outputPath)
    } catch {
      case e: Exception =>
        println(s"Error appending data to Delta table: ${e.getMessage}")
        e.printStackTrace()
    }

    MetabolicWriter.write(input, sink, true, false, "", EngineMode.Batch, Seq.empty[String] )(spark, region)

    val result = spark.read.delta(outputPath)
      .selectExpr("name","age","updated_at", "cast(yyyy as int) ", "cast(mm as int)", "cast(dd as int)", "version")

    assertDataFrameNoOrderEquals(expected, result)

  }

  test("Write Delta Batch - Overwrite") {

    val outputPath = "src/test/tmp/delta/out_fake_employees_delta_overwrite"
    val tableName = "employees_overwrite_fake"

    val sink = getFileSink(outputPath, tableName, IOFormat.DELTA.toString, "replace")

    val expected = getFakeEmployeesDataframe()
      .selectExpr("name", "age", "updated_at", "yyyy", "mm", "dd", "version")

    val input = expected
      .select(
        col("name"),
        col("age"),
        col("updated_at"),
        col("version")
      )

    if (!DeltaTable.isDeltaTable(outputPath)) {
      try {
        val deltaTable = DeltaTable.createIfNotExists()
          .tableName(tableName)
          .location(outputPath)
          .addColumns(input.schema)
          .execute()

        if (DeltaTable.isDeltaTable(outputPath)) {
          println(s"Table $tableName created successfully.")
        } else {
          println("Table creation failed, it may be in an incomplete state.")
        }
      } catch {
        case e: Exception =>
          println(s"Error creating Delta table: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    try {
      println("Appending data to Delta table.")
      val deltaTable = DeltaTable.forPath(outputPath) // Use forPath to reference the existing table
      deltaTable.toDF.write.format("delta").mode(SaveMode.Append).save(outputPath)
    } catch {
      case e: Exception =>
        println(s"Error appending data to Delta table: ${e.getMessage}")
        e.printStackTrace()
    }

    MetabolicWriter.write(input, sink, false, false, "", EngineMode.Batch, Seq.empty[String])(spark, region)

    val result = spark.read.delta(outputPath)
      .selectExpr("name", "age", "updated_at", "cast(yyyy as int) ", "cast(mm as int)", "cast(dd as int)", "version")

    assertDataFrameNoOrderEquals(expected, result)

  }

}

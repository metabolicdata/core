package com.metabolic.data.mapper.app

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops.SQLStatmentMapping
import com.metabolic.data.mapper.domain.{Config, io}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FileFormatsIT extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

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
      FileSink("test", "src/test/tmp_formats/il_fake_parquet_employees_t", SaveMode.Overwrite, IOFormat.PARQUET)
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
      io.FileSink("test", "src/test/tmp_formats/il_fake_json_employees_t", SaveMode.Overwrite, IOFormat.JSON)
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

  ignore("Write delta") {

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
      .format("delta")
      .save("src/test/tmp_formats/fake_delta_employees")

    val multilineSQL = """|select *
                          |from employees""".stripMargin

    val testingConfig = Config(
      "",
      List(io.FileSource("src/test/tmp_formats/fake_delta_employees", "employees", format = IOFormat.DELTA)),
      List(SQLStatmentMapping(multilineSQL)),
      io.FileSink("test", "src/test/tmp_formats/il_fake_delta_employees_t", SaveMode.Overwrite, IOFormat.DELTA,
        Option("name"), Option("age"))
    )

    MetabolicApp()
      .transformAll(List(testingConfig))(region, spark)

    val NT_fakeEmployeesDF = spark.read.format("delta")
      .load("src/test/tmp_formats/il_fake_delta_employees_t")
    fakeEmployeesDF.show()
    NT_fakeEmployeesDF.show()
    assertDataFrameNoOrderEquals(fakeEmployeesDF, NT_fakeEmployeesDF)
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
      List(MetastoreSource("fake_json_employees", "employees")),
      List(SQLStatmentMapping(multilineSQL)),
      io.FileSink("test", "src/test/tmp_formats/table_fake_json_employees_t", SaveMode.Overwrite, IOFormat.JSON)
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

}

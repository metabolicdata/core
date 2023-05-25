package com.metabolic.data.core.services.spark.filter

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DateComponentsFromReaderTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll {

  val inputData = Seq(
    Row("Marc 1", 2021, 6, 15),
    Row("Pau 1", 2020, 3, 7),
    Row("Enric 1", 2019, 9, 30),
    Row("Marc 2", 2019, 6, 15),
    Row("Pau 2", 2021, 3, 7),
    Row("Enric 2", 2020, 9, 30),
    Row("Marc 3", 2020, 6, 15),
    Row("Pau 3", 2019, 3, 7),
    Row("Enric 3", 2021, 9, 30)
  )

  val someSchema = List(
    StructField("name", StringType, true),
    StructField("yyyy", IntegerType, true),
    StructField("mm", IntegerType, true),
    StructField("dd", IntegerType, true)
  )

  test("Filters rows with 2020+ year in") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val outputDF = inputDF
      .transform(new DateComponentsFromReader(new DateTime(1590999363000L), depth = DataLakeDepth.YEAR).filter())

    val expectedData = Seq(
      Row("Marc 1", 2021, 6, 15),
      Row("Pau 1", 2020, 3, 7),
      Row("Pau 2", 2021, 3, 7),
      Row("Enric 2", 2020, 9, 30),
      Row("Marc 3", 2020, 6, 15),
      Row("Enric 3", 2021, 9, 30)
    )

    val expextedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expextedDF)

  }

  test("Filters rows with 2020-06+ month in") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val outputDF = inputDF
      .transform(new DateComponentsFromReader(new DateTime(1590999363000L), depth = DataLakeDepth.MONTH).filter())

    val expectedData = Seq(
      Row("Marc 1", 2021, 6, 15),
      Row("Pau 2", 2021, 3, 7),
      Row("Enric 2", 2020, 9, 30),
      Row("Marc 3", 2020, 6, 15),
      Row("Enric 3", 2021, 9, 30)
    )

    val expextedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expextedDF)

  }

  test("Filters rows with 2020-06-01+ day in") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val outputDF = inputDF
      .transform(new DateComponentsFromReader(new DateTime(1592640963000L), depth = DataLakeDepth.DAY).filter())

    val expectedData = Seq(
      Row("Marc 1", 2021, 6, 15),
      Row("Pau 2", 2021, 3, 7),
      Row("Enric 2", 2020, 9, 30),
      Row("Enric 3", 2021, 9, 30)
    )

    val expextedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expextedDF)

  }

}
